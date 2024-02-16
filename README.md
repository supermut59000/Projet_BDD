# Projet_BDD #

## Description du projet ##


## MCD de la base Entité-Association ##
![alt tag](https://github.com/supermut59000/Projet_BDD/blob/amar/Assets/MCD_Entit%C3%A9s_Associations_RESLINGER_BENNOUR.PNG) 
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
![alt tag](https://github.com/supermut59000/Projet_BDD/blob/amar/Assets/MLD_RESLINGER_BENNOUR.PNG?raw=true)
## Exécution de la création de la base Entité-Association ##

Pour exécuter le script de création deux solutions :
- Utiliser le CreateBDD.exe
- Utiliser le CreateBDD.py
