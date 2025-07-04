-- This file should contain all code required to create & seed database tables.

DROP TABLE IF EXISTS rating_interaction;
DROP TABLE IF EXISTS rating;
DROP TABLE IF EXISTS request_interaction;
DROP TABLE IF EXISTS request;
DROP TABLE IF EXISTS exhibition;
DROP TABLE IF EXISTS department;
DROP TABLE IF EXISTS floor;

-- Create tables
CREATE TABLE department (
    department_id SMALLINT UNIQUE NOT NULL,
    department_name VARCHAR(100),
    PRIMARY KEY (department_id)
);

CREATE TABLE floor (
    floor_id SMALLINT UNIQUE NOT NULL,
    floor_name VARCHAR(100),
    PRIMARY KEY (floor_id)
);

CREATE TABLE exhibition (
    exhibition_id SMALLINT UNIQUE NOT NULL,
    exhibition_name VARCHAR(100),
    exhibition_description TEXT,
    department_id SMALLINT,
    floor_id SMALLINT,
    exhibition_start_date DATE,
    public_id TEXT,
    PRIMARY KEY (exhibition_id),
    FOREIGN KEY (department_id) REFERENCES department (department_id),
    FOREIGN KEY (floor_id) REFERENCES floor (floor_id),
    -- ensures public_id EXH_01 to EXH_10 match exhibition_id 1 to 10
    CONSTRAINT matching_ids CHECK (
        RIGHT(public_id, 2) = TO_CHAR(exhibition_id, 'FM00'))
);

CREATE TABLE rating (
    rating_id SMALLINT UNIQUE NOT NULL,
    rating_value SMALLINT,
    rating_description VARCHAR(100),
    PRIMARY KEY (rating_id)
);

CREATE TABLE rating_interaction (
    rating_interaction_id BIGINT GENERATED ALWAYS AS IDENTITY,
    exhibition_id SMALLINT,
    rating_id SMALLINT,
    event_at TIMESTAMPTZ,
    PRIMARY KEY (rating_interaction_id),
    FOREIGN KEY (exhibition_id) REFERENCES exhibition (exhibition_id),
    FOREIGN KEY (rating_id) REFERENCES rating (rating_id)
);

CREATE TABLE request (
    request_id SMALLINT UNIQUE NOT NULL,
    request_value SMALLINT,
    request_description VARCHAR(100),
    PRIMARY KEY (request_id)
);

CREATE TABLE request_interaction (
    request_interaction_id BIGINT GENERATED ALWAYS AS IDENTITY,
    exhibition_id SMALLINT,
    request_id SMALLINT,
    event_at TIMESTAMPTZ,
    PRIMARY KEY (request_interaction_id),
    FOREIGN KEY (exhibition_id) REFERENCES exhibition (exhibition_id),
    FOREIGN KEY (request_id) REFERENCES request (request_id)
);

-- Seed master data
INSERT INTO department (department_id, department_name) 
VALUES (1, 'Ecology'),
(2, 'Entomology'),
(3, 'Geology'),
(4, 'Paleontology'),
(5, 'Zoology');

INSERT INTO floor (floor_id, floor_name) 
VALUES (0, 'Vault'),
(1, '1'),
(2, '2'),
(3, '3');

INSERT INTO exhibition (exhibition_id, exhibition_name, exhibition_description,
    department_id, floor_id, exhibition_start_date, public_id) VALUES
(0, 'Measureless to Man', 'An immersive 3D experience: delve deep into a previously-inaccessible cave system.',
3, 1, '2021-08-23', 'EXH_00'),
(1, 'Adaptation', 'How insect evolution has kept pace with an industrialised world',
2, 0, '2019-07-01', 'EXH_01'),
(2, 'The Crenshaw Collection', 'An exhibition of 18th Century watercolours, mostly focused on South American wildlife.',
5, 2, '2021-03-03', 'EXH_02'),
(3, 'Cetacean Sensations', 'Whales: from ancient myth to critically endangered.',
5, 1, '2019-07-01', 'EXH_03'),
(4, 'Our Polluted World', 'A hard-hitting exploration of humanity''s impact on the environment.',
1, 3, '2021-05-12', 'EXH_04'),
(5, 'Thunder Lizards', 'How new research is making scientists rethink what dinosaurs really looked like.',
4, 1, '2023-02-01', 'EXH_05');


INSERT INTO rating (rating_id, rating_value, rating_description) VALUES
(0, 0, 'Terrible'),
(1, 1, 'Bad'),
(2, 2, 'Neutral'),
(3, 3, 'Good'),
(4, 4, 'Amazing');

INSERT INTO request (request_id, request_value, request_description) VALUES
(0, 0, 'Assistance'),
(1, 1, 'Emergency');