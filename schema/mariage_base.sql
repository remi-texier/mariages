-- Maëlle LE LANNIC / Rémi TEXIER	
DROP TABLE IF EXISTS Actes CASCADE;
DROP TABLE IF EXISTS Personnes CASCADE;
DROP TABLE IF EXISTS Communes CASCADE;
DROP TABLE IF EXISTS Type_acte CASCADE;
DROP TYPE IF EXISTS num_dept CASCADE;

CREATE TABLE personnes (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    prenom VARCHAR(255) NOT NULL,
    prenom_pere VARCHAR(255),
    nom_mere VARCHAR(255),
    prenom_mere VARCHAR(255)
);

CREATE TABLE communes (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    num_dept INT NOT NULL
);

CREATE TABLE type_acte (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(255) NOT NULL
);

CREATE TABLE actes (
	id SERIAL PRIMARY KEY,
	type_acte INT NOT NULL REFERENCES Type_acte(id),
	p1 INT REFERENCES Personnes(id),
    p2 INT REFERENCES Personnes(id),
    commune INT NOT NULL REFERENCES Communes(id),
	date DATE,
	num_vue VARCHAR(255)
);