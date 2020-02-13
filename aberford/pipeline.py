from abc import ABC


class AbstractPipelineStage(ABC):

    def __init__(self, *args, **kwargs):
        """
        Initializing stage configuration.
        """
        raise NotImplementedError

    def __call__(self):
        """
        Actual stage implementation.
        """
        raise NotImplementedError

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)


class CompoundPipelineStage(AbstractPipelineStage, ABC):

    def __init__(self, one: AbstractPipelineStage, two: AbstractPipelineStage):
        self.one = one
        self.two = two


class And(CompoundPipelineStage):

    def __call__(self, *args, **kwargs):
        return self.one() and self.two()


class Or(CompoundPipelineStage):

    def __call__(self, *args, **kwargs):
        return self.one() or self.two()


def run():
    # @TODO:
    #  pipeline = Parse('src') & (
    #      (ProceedIf(extension='html') & Jinja2() & Write('out')) |
    #      (ProceedIf(extension='jpg') & Verbatim() & Write('out')) |
    #      Fail()
    #  )

    raise NotImplementedError
