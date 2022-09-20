-- create pet table
CREATE TABLE IF NOT EXISTS `line` (
    DataOwnerCode VARCHAR(255) NOT NULL,
    LinePlanningNumber VARCHAR(255) NOT NULL,
    LineDirection SMALLINT NOT NULL,
    LinePublicNumber VARCHAR(255),
    LineName VARCHAR(255),
    DestinationName50 VARCHAR(255),
    DestinationCode VARCHAR(255),
    LineWheelchairAccessible VARCHAR(255),
    TransportType VARCHAR(255),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    -- composite primary key
    PRIMARY KEY(DataOwnerCode, LinePlanningNumber, LineDirection);
