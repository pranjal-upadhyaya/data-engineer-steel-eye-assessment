from datetime import datetime

from pydantic import BaseModel, Field


class ESMARegistersFileModel(BaseModel):
    """Pydantic model representing a single file record from the ESMA FIRDS Solr API response."""

    root: str = Field(
        alias="_root_", description="Root identifier of the Solr document"
    )
    id: str = Field(description="Unique identifier of the file record")
    published_instrument_file_id: str = Field(
        description="Published instrument file ID, matches the record ID"
    )
    file_name: str = Field(
        description="Name of the zip file containing the instrument data"
    )
    file_type: str = Field(
        description="Type of the file, e.g. DLTINS for daily list of trading instruments"
    )
    publication_date: datetime = Field(
        description="ISO 8601 datetime when the file was published"
    )
    download_link: str = Field(description="Direct URL to download the zip file")
    checksum: str = Field(
        description="MD5 checksum for verifying file integrity after download"
    )
    version: str = Field(
        alias="_version_", description="Solr internal version number of the document"
    )
    timestamp: str = Field(
        description="ISO 8601 datetime when the Solr record was last updated"
    )

    model_config = {"populate_by_name": True}
