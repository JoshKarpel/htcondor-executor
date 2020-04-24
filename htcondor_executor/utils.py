import enum


class StrEnum(enum.Enum):
    def __str__(self):
        return self.value
