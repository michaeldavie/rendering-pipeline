import aws_cdk as core
import aws_cdk.assertions as assertions

from rendering_pipeline.rendering_pipeline_stack import RenderingPipelineStack

# example tests. To run these tests, uncomment this file along with the example
# resource in rendering_pipeline/rendering_pipeline_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = RenderingPipelineStack(app, "rendering-pipeline")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
