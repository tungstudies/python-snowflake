from db.snowflake.sqlalchemy.baseclass import Base
from sqlalchemy import Column, Integer, String


class Region(Base):
    """
    Defines the regions model
    """

    __tablename__ = "tbl_regions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, unique=True)
    two_letter_abbr = Column(String(2), index=True, nullable=True)
    three_letter_abbr = Column(String(3), index=True, nullable=True)
    currency_code = Column(String(3), nullable=True)

    def __str__(self) -> str:
        return f"<Region {self.name} ({self.currency_code})"
