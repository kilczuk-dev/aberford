from abc import ABC
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from typing import Dict, List


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
    entities: List[PipelineContextEntity] = []
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
        if ctx_old is None:
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

    def __init__(self, one: AbstractPipelineStage, two: AbstractPipelineStage):
        self.one = one
        self.two = two


class And(CompoundPipelineStage):

    def run(self, ctx: PipelineContext) -> PipelineContext:
        ctx_one = self.one(ctx)
        if ctx_one.can_continue:
            return self.two(ctx_one)
        return ctx_one


class Or(CompoundPipelineStage):

    def run(self, ctx: PipelineContext) -> PipelineContext:
        ctx_one = self.one(ctx)
        if ctx_one.can_continue:
            return ctx_one
        return self.two(ctx)
