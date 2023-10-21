from aws_cdk import (
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_efs as efs,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sns as sns,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct


class RenderingPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # VPC

        render_vpc = ec2.Vpc(self, "render_vpc")

        # IAM

        lambda_execution_role = iam.Role(
            self,
            "lambda_execution_role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaENIManagementAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonElasticFileSystemClientReadWriteAccess"
                ),
            ],
        )

        batch_execution_role = iam.Role(
            self,
            "batch_execution_role",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )

        batch_job_role = iam.Role(
            self,
            "batch_job_role",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        # EFS

        render_file_system = efs.FileSystem(
            self,
            "render_file_system",
            vpc=render_vpc,
            file_system_name="render_file_system",
            removal_policy=RemovalPolicy.DESTROY,
            allow_anonymous_access=True,
        )

        access_point = render_file_system.add_access_point(
            "render_access_point",
            path="/data",
            create_acl=efs.Acl(
                owner_uid="1001",
                owner_gid="1001",
                permissions="777",
            ),
            posix_user=efs.PosixUser(uid="1001", gid="1001"),
        )

        lambda_file_system = lambda_.FileSystem.from_efs_access_point(
            ap=access_point, mount_path="/mnt/data"
        )

        # Lambda

        s3_to_efs_lambda = lambda_.Function(
            self,
            "s3_to_efs_lambda",
            code=lambda_.Code.from_asset("assets/lambda_s3_to_efs"),
            handler="s3_to_efs.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(300),
            memory_size=512,
            vpc=render_vpc,
            filesystem=lambda_file_system,
            role=lambda_execution_role,
        )

        count_frames_lambda = lambda_.Function(
            self,
            "count_frames_lambda",
            code=lambda_.Code.from_asset("assets/lambda_count_frames"),
            handler="count_frames.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(300),
            memory_size=512,
            vpc=render_vpc,
            filesystem=lambda_file_system,
            role=lambda_execution_role,
        )

        # S3

        input_bucket = s3.Bucket(
            self,
            "input_bucket",
            event_bridge_enabled=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        input_bucket.grant_read(lambda_execution_role)

        output_bucket = s3.Bucket(
            self,
            "output_bucket",
            event_bridge_enabled=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        output_bucket.grant_write(batch_job_role)

        # Batch

        docker_image = ecr_assets.DockerImageAsset(
            self,
            "docker_image",
            directory="assets/docker",
            platform=ecr_assets.Platform.LINUX_AMD64,
        )

        batch_job_definition = batch.EcsJobDefinition(
            self,
            "batch_job_definition",
            container=batch.EcsFargateContainerDefinition(
                self,
                "container",
                image=ecs.ContainerImage.from_docker_image_asset(docker_image),
                memory=Size.gibibytes(8),
                cpu=1,
                execution_role=batch_execution_role,
                job_role=batch_job_role,
                volumes=[
                    batch.EfsVolume(
                        name="data",
                        file_system=render_file_system,
                        root_directory="/data",
                        container_path="/data",
                    )
                ],
                command=[
                    "Ref::action",
                    "-i",
                    "Ref::inputUri",
                    "-o",
                    "Ref::outputUri",
                    "-f",
                    "Ref::framesPerJob",
                ],
            ),
        )

        fargate_compute_env = batch.FargateComputeEnvironment(
            self, "fargate_compute_env", vpc=render_vpc
        )

        render_file_system.connections.allow_from(
            fargate_compute_env.connections, ec2.Port.tcp(2049)
        )

        batch_job_queue = batch.JobQueue(
            self,
            "batch_job_queue",
            compute_environments=[
                {"computeEnvironment": fargate_compute_env, "order": 1}
            ],
        )

        # State Machine

        input_state = sfn.Pass(
            self,
            "pass_input_state",
            parameters={
                "jobName.$": "States.ArrayGetItem(States.StringSplit($.detail.object.key, '.'), 0)",
                "inputUri.$": "States.Format('s3://"
                + input_bucket.bucket_name
                + "/{}', $.detail.object.key)",
                "outputUri.$": "States.Format('s3://"
                + output_bucket.bucket_name
                + "/{}', $.detail.object.key)",
                "jobDefinitionArn": batch_job_definition.job_definition_arn,
                "jobQueueArn": batch_job_queue.job_queue_arn,
                "framesPerJob": "1",
            },
        )

        s3_to_efs_state = sfn_tasks.LambdaInvoke(
            self,
            "s3_to_efs_state",
            lambda_function=s3_to_efs_lambda,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_selector={"blend_file.$": "$.Payload.body"},
            result_path="$.blend_file",
        )

        count_frames_state = sfn_tasks.LambdaInvoke(
            self,
            "count_frames_state",
            lambda_function=count_frames_lambda,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.output",
        )

        render_state = sfn_tasks.BatchSubmitJob(
            self,
            "render_state",
            job_name="render",
            array_size=sfn.JsonPath.number_at("$.output.Payload.body.arrayJobSize"),
            job_definition_arn=batch_job_definition.job_definition_arn,
            job_queue_arn=batch_job_queue.job_queue_arn,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "render",
                    "inputUri": sfn.JsonPath.string_at("$.blend_file.blend_file"),
                    "outputUri": sfn.JsonPath.string_at("$.outputUri"),
                    "framesPerJob": sfn.JsonPath.string_at("$.framesPerJob"),
                }
            ),
            result_path="$.output",
        )

        stitching_state = sfn_tasks.BatchSubmitJob(
            self,
            "stitching_state",
            job_name="stitching",
            job_definition_arn=batch_job_definition.job_definition_arn,
            job_queue_arn=batch_job_queue.job_queue_arn,
            payload=sfn.TaskInput.from_object(
                {
                    "action": "stitch",
                    "inputUri": sfn.JsonPath.string_at("$.inputUri"),
                    "outputUri": sfn.JsonPath.string_at("$.outputUri"),
                    "framesPerJob": sfn.JsonPath.string_at("$.framesPerJob"),
                }
            ),
        )

        state_chain = (
            input_state.next(s3_to_efs_state)
            .next(count_frames_state)
            .next(render_state)
            .next(stitching_state)
        )

        render_state_machine = sfn.StateMachine(
            self,
            "render_state_machine",
            definition_body=sfn.DefinitionBody.from_chainable(state_chain),
            state_machine_name="render_state_machine",
        )

        # EventBridge

        trigger_on_upload = events.Rule(
            self,
            "trigger_on_upload",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={"bucket": {"name": [input_bucket.bucket_name]}},
            ),
            targets=[events_targets.SfnStateMachine(render_state_machine)],
        )
