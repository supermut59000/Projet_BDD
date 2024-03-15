CREATE TABLE invoice_dim(
   TPK_invoice INTEGER PRIMARY KEY AUTOINCREMENT,
   invoice_id INT NOT NULL,
   billing_address VARCHAR(50),
   billing_city VARCHAR(50),
   billing_state VARCHAR(50),
   billing_country VARCHAR(50),
   billing_postal_code VARCHAR(50),
   total INT
);

CREATE TABLE track_dim(
   TPK_track INTEGER PRIMARY KEY AUTOINCREMENT,
   track_id INT NOT NULL,
   name VARCHAR(50),
   composer VARCHAR(50),
   milliseconds INT,
   bytes INT,
   unit_price DECIMAL(15,2),
   media_type VARCHAR(50),
   genre VARCHAR(50),
   album VARCHAR(50),
   artist VARCHAR(50)
);

CREATE TABLE date_dim(
   date_id INT,
   Date_D DATE,
   year_D VARCHAR(50),
   month_D INT,
   MonthNumberOfDay INT,
   day_D INT,
   day_name VARCHAR(50),
   day_week INT,
   week INT,
   quarter INT,
   PRIMARY KEY(date_id)
);

CREATE TABLE customer_dim(
   TPK_customer INTEGER PRIMARY KEY AUTOINCREMENT,
   customer_id INT NOT NULL,
   first_name VARCHAR(50),
   last_name VARCHAR(50),
   company VARCHAR(50),
   address VARCHAR(50),
   city VARCHAR(50),
   state VARCHAR(50),
   country VARCHAR(50),
   postal_code VARCHAR(50),
   phone VARCHAR(50),
   fax VARCHAR(50),
   email VARCHAR(50),
   support_rep_id INT
);

CREATE TABLE employe_dim(
   TPK_employe INTEGER PRIMARY KEY AUTOINCREMENT,
   EmployeeId INT NOT NULL,
   LastName VARCHAR(50),
   FirstName VARCHAR(50),
   Title VARCHAR(50),
   BirthDate VARCHAR(50),
   HireDate VARCHAR(50),
   Address VARCHAR(50),
   City VARCHAR(50),
   State VARCHAR(50),
   Country VARCHAR(50),
   PostalCode VARCHAR(50),
   Phone VARCHAR(50),
   Fax VARCHAR(50),
   Email VARCHAR(50)
);

CREATE TABLE invoice_fact(
   invoice_line_id INTEGER PRIMARY KEY AUTOINCREMENT,
   quantity INT,
   TPK_customer INT,
   TPK_track INT,
   date_id INT,
   TPK_invoice INT,
   TPK_employe INT,
   FOREIGN KEY(TPK_customer) REFERENCES customer_dim(TPK_customer),
   FOREIGN KEY(TPK_track) REFERENCES track_dim(TPK_track),
   FOREIGN KEY(date_id) REFERENCES date_dim(date_id),
   FOREIGN KEY(TPK_invoice) REFERENCES invoice_dim(TPK_invoice),
   FOREIGN KEY(TPK_employe) REFERENCES employe_dim(TPK_employe)
);
