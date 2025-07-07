from typing import TypeVar, Iterable
from typing import Dict, Any, Hashable, List, Union

from torch import Tensor

T = TypeVar("T", bound="ChildrenTrackable")

Row = Dict[Hashable, Any]
Batch = Dict[Hashable, Union[List[Any], Tensor]]


class ChildrenTrackable:
    """
    A base class for classes that provide a children method to return an iterable of child objects.
    """

    @classmethod
    def children(cls: type[T]) -> Iterable[type[T]]:
        """
        Returns an iterable of child objects.
        This method should be implemented by subclasses.
        """
        def recursive_children() -> Iterable[type[T]]:
            for child in cls.__subclasses__():
                yield child
                yield from child.__subclasses__()
        return recursive_children()

    @classmethod
    def children_names(cls: type[T]) -> List[str]:
        """
        Returns a list of names of child classes.
        This method is useful for debugging and introspection.
        """
        return [child.__name__ for child in cls.children()]

    @classmethod
    def children_dict(cls: type[T]) -> Dict[str, type[T]]:
        """
        Returns a dictionary mapping child class names to child classes.
        This method is useful for debugging and introspection.
        """
        name_dict: Dict[str, type[T]] = {}
        for child in cls.children():
            if child.__name__ in name_dict:
                raise ValueError(f"Duplicate child class name: {child.__name__}")
            name_dict[child.__name__] = child
        return name_dict

    @classmethod
    def get_child(cls: type[T], name: str) -> type[T]:
        """
        Returns the child class with the given name.
        Raises a KeyError if the name is not found.
        """
        children_dict = cls.children_dict()
        if name not in children_dict:
            raise KeyError(f"Child class '{name}' not found in {cls.__name__}.")
        return children_dict[name]


class CouldCountable:
    """
    A base class for iterators that can be counted.
    A counted class should implement the __len__ method.
    """

    def is_countable(self) -> bool:
        """
        Returns True if the class is countable, meaning it implements the __len__ method.
        """
        return hasattr(self, '__len__') and callable(getattr(self, '__len__'))
