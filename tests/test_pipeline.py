from aberford import pipeline


class SuccessfulStage(pipeline.AbstractPipelineStage):

    def run(self, ctx: pipeline.PipelineContext) -> pipeline.PipelineContext:
        ctx['successes'] = 1 if 'successes' not in ctx else ctx['successes'] + 1
        return ctx


class FailingStage(pipeline.AbstractPipelineStage):

    def run(self, ctx: pipeline.PipelineContext):
        ctx['failures'] = 1 if 'failures' not in ctx else ctx['failures'] + 1
        raise pipeline.Stop


def test_pipeline_can_be_completed_and_or():
    """
    Ensures that pipeline can be completed when using a mixture of AND and OR.

    In this case:
        1. The first SuccessfulStage completes successfully and the pipeline continues
        2. The FailingStage fails, but because of the or the pipeline continues
        3. The second SuccessfulStage completes
        4. The result of the pipeline is the output of the second SuccessfulStage
        5. The context of FailingStage is silently discarded
    """
    pl = SuccessfulStage() & (
        FailingStage() | SuccessfulStage()
    )
    final_context = pl()
    assert final_context.data['successes'] == 2
