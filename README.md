# Projet_BDD #

## MCD de la base Entité-Association ##
<img src="https://github.com/supermut59000/Projet_BDD/blob/main/Assets/MCD_Entit%C3%A9s_Associations_RESLINGER_BENNOUR.PNG" alt="Texte alternatif" width="700" height="500">

## MLD de la base Entité-Association ##
```sql
Employee = (EmployeeId INT, LastName VARCHAR(50), FirstName VARCHAR(50), Title VARCHAR(50), BirthDate VARCHAR(50), HireDate VARCHAR(50), Address VARCHAR(50), City VARCHAR(50), State VARCHAR(50), Country VARCHAR(50), PostalCode VARCHAR(50), Phone VARCHAR(50), Fax VARCHAR(50), Email VARCHAR(50), #EmployeeId_1);
Genre = (GenreId INT, Name VARCHAR(50));
Artist = (ArtistId INT, Name VARCHAR(50));
MediaType = (MediaTypeId INT, Name VARCHAR(50));
Playlist = (PlaylistId INT, Name VARCHAR(50));
Customer = (CustomerId INT, FirstName VARCHAR(50), LastName VARCHAR(50), Company VARCHAR(50), Address VARCHAR(50), City VARCHAR(50), State VARCHAR(50), Country VARCHAR(50), PostalCode VARCHAR(50), Phone VARCHAR(50), Fax VARCHAR(50), Email VARCHAR(50), #EmployeeId*);
Album = (AlbumId INT, Title VARCHAR(50), #ArtistId);
Invoice = (InvoiceId INT, InvoiceDate VARCHAR(50), BillingAddress VARCHAR(50), BilllingCity VARCHAR(50), BillingState VARCHAR(50), BillingCountry VARCHAR(50), BillingPostalCode VARCHAR(50), Total VARCHAR(50), #CustomerId*);
Track = (TrackId INT, Name VARCHAR(50), Composer VARCHAR(50), Miliseconds INT, Bytes INT, UnitPrice DECIMAL(3,2), #AlbumId*, #GenreId*, #MediaTypeId*);
InvoiceLine = (InvoiceLineId INT, unitPrice DECIMAL(3,2), Quantity INT, #InvoiceId, #TrackId*);
PlaylistTrack = (#TrackId, #PlaylistId);
```
<img src="https://github.com/supermut59000/Projet_BDD/blob/main/Assets/MLD_RESLINGER_BENNOUR.PNG" alt="Texte alternatif" width="700" height="500">

## Exécution de la création de la base Entité-Association ##

Pour exécuter le script de création deux solutions :
- Utiliser le CreateBDD.exe
- Utiliser le CreateBDD.py

Une fois le script executé, ouvrir le fichier OP.db avec un interface de base de donnée.


## Schéma du DataWare House ##
<img src="https://github.com/supermut59000/Projet_BDD/blob/main/Assets/SchemaDWH_RESLINGER_BENNOUR.PNG" alt="Texte alternatif" width="700" height="500">

##  Exécution de la création du DataWare House ##

Pour exécuter le script de création deux solutions :
- Utiliser le CreateDWH.exe
- Utiliser le CreateDWH.py

Une fois le script executé, un nouveau dossier Metadata contenant l'ensemble des tables ainsi qu'une colonne permettant l'application ou non du SCD2 sur celle-ci.\
La dimension Track a le SCD2 appliqué  automatiquement.

##  Alimentation du DataWare House ##

Exécuter le script AlimentationDWH.py afin d'alimenter le dataware house à partir de la base Entité-Association.

##  Mise à jour de la base Entité-Association  ##

Exécuter le script main.py afin de mettre à jour la base Entité-Association.

## Re-alimentation du DataWare House ##

Lors de la seconde alimentation du DataWare House et donc seconde exécution  du script AlimentationDWH.py, le SCD2 s'applique sur les tables spécifié  au sein du fichier metadata.

## Bonus ##
### Ajout des liens youtube ###
Nous avons créer un script permettant d'ajouter un lien vers la vidéo YouTube associer à une musique.
Le fichier contenant le code s'appelle "APICALL"
