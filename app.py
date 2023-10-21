#!/usr/bin/env python3
import aws_cdk as cdk

from rendering_pipeline.rendering_pipeline_stack import RenderingPipelineStack


app = cdk.App()
RenderingPipelineStack(app, "RenderingPipelineStack")

app.synth()
