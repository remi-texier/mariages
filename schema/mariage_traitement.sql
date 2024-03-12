-- Maëlle LE LANNIC / Rémi TEXIER
-- Test insertion des données
SELECT * FROM Communes;
SELECT * FROM Type_acte;
SELECT * FROM Personnes;
SELECT * FROM Actes;

-- La quantité de communes par département
SELECT cm.num_dept, COUNT(*) AS nb_communes_dept
FROM Communes cm
GROUP BY num_dept;

-- La quantité de actes à LUÇON
SELECT COUNT(*) AS nb_actes_lu
FROM Actes ac
JOIN Communes cm ON ac.commune = cm.id
WHERE cm.nom = 'LUÇON';

-- La quantité de “contrat de mariage” avant 1855
SELECT COUNT(*) AS nb_cm_av1855
FROM Actes ac
JOIN Type_acte ta ON ac.type_acte = ta.id
WHERE ta.nom='Contrat de mariage' AND ac.date < '01/01/1855';

-- La commune avec la plus quantité de “publications de mariage”
SELECT cm.nom, COUNT(*)
FROM Actes ac
JOIN Communes cm ON ac.commune = cm.id
JOIN Type_acte ta ON ac.type_acte = ta.id
WHERE ta.nom = 'Publication de mariage'
GROUP BY cm.nom
ORDER BY COUNT(*) DESC
LIMIT 1;

-- La date du premier acte et le dernier acte
SELECT MIN(ac.date) AS Premier_acte, MAX(ac.date) AS Dernier_acte
FROM Actes ac