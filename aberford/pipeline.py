from abc import ABC
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, Sized, Collection, List


class Stop(Exception):
    pass


class DictLike:

    data: Dict

    def __init__(self):
        self.data = {}

    def __contains__(self, key):
        return key in self.data

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value


class PipelineContextEntity(DictLike):

    location: Path
    content: BytesIO  # @TODO: ???
    data: Dict


class PipelineContext(DictLike):

    can_continue: bool = True
    entities: Iterable[PipelineContextEntity] = []
    data: Dict

    def clone(self):
        return deepcopy(self)

    def __bool__(self):
        return bool(self.can_continue)


class AbstractPipelineStage(ABC):

    def __init__(self, *args, **kwargs):
        """
        Initializing stage configuration. This is optional.
        """
        pass

    def __call__(self, ctx_old: PipelineContext = None):
        """
        Actual stage implementation.
        """
        if ctx_old is None:  # in the initial stage, context is fully optional
            ctx_old = PipelineContext()
        try:
            return self.run(ctx_old.clone())
        except Stop:
            ctx_old.can_continue = False
            return ctx_old

    def run(self, ctx: PipelineContext) -> PipelineContext:
        raise NotImplementedError

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)


class CompoundPipelineStage(AbstractPipelineStage, ABC):

    subpipelines: Collection[AbstractPipelineStage]

    def __init__(self, *subpipelines: AbstractPipelineStage):
        if len(subpipelines) < 2:
            raise ValueError(f'{self.__class__.__name__} needs at least two inner pipelines.')
        self.subpipelines = subpipelines
        self.stage_iter = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.stage_iter > len(self.subpipelines):
            raise StopIteration
        stage = self.subpipelines[self.stage_iter]
        self.stage_iter += 1
        return stage


class And(CompoundPipelineStage):

    def run(self, ctx: PipelineContext) -> PipelineContext:
        ctx_one = next(self)(ctx)
        if ctx_one.can_continue:
            return next(self)(ctx_one)
        return ctx_one


class Or(CompoundPipelineStage):

    def run(self, ctx: PipelineContext) -> PipelineContext:
        ctx_one = next(self)(ctx)
        if ctx_one.can_continue:
            return ctx_one
        return next(self)(ctx)


class Diverge(CompoundPipelineStage):

    def run(self, ctx: PipelineContext) -> List[PipelineContext]:
        return [pipeline(ctx) for pipeline in self.subpipelines]
