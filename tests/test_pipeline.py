from aberford.pipeline import Diverge, AbstractPipelineStage, PipelineContext, Stop


class SuccessfulStage(AbstractPipelineStage):

    def run(self, ctx: PipelineContext) -> PipelineContext:
        ctx['successes'] = 1 if 'successes' not in ctx else ctx['successes'] + 1
        return ctx


class FailingStage(AbstractPipelineStage):

    def run(self, ctx: PipelineContext):
        ctx['failures'] = 1 if 'failures' not in ctx else ctx['failures'] + 1
        raise Stop


def test_pipeline_can_be_completed_and_or():
    """
    Ensures that a pipeline can be completed when using a mixture of AND and OR.

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


def test_pipeline_diverge():
    """
    Ensures that a pipeline can be completed when using Diverge.

    In this case:
        1. The first SuccessfulStage completes successfully and the pipeline continues
        2. The context of the first SuccessfulStage is passed to Diverge
        3. The pipeline branches into two
        4. The second and third SuccessfulStages pass successfully
        5. The fourth SuccessfulStage passes successfully
        6. The result of the pipeline are the results of third and fourth SuccessfulStages, combined into a list
    """
    pl = SuccessfulStage() & Diverge(
        (SuccessfulStage() & SuccessfulStage()),  # second, third SuccessfulStage
        SuccessfulStage()  # fourth SuccessfulStage
    )
    final_context = pl()
    assert isinstance(final_context, list)
    assert len(final_context) == 2

    first_branch = final_context[0]
    assert first_branch['successes'] == 3

    second_branch = final_context[1]
    assert second_branch['successes'] == 2
