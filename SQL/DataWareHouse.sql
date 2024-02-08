/*******************************************************************************
   Drop Tables
********************************************************************************/
DROP TABLE IF EXISTS [invoice_dim];

DROP TABLE IF EXISTS [track_dim];

DROP TABLE IF EXISTS [date_dim];

DROP TABLE IF EXISTS [customer_dim];

DROP TABLE IF EXISTS [invoice_fact];

/*******************************************************************************
   Create Tables
********************************************************************************/

CREATE TABLE invoice_dim(
   invoice_id INT,
   billing_address VARCHAR(50),
   billing_city VARCHAR(50),
   billing_state VARCHAR(50),
   billing_country VARCHAR(50),
   billing_postal_code VARCHAR(50),
   total INT,
   PRIMARY KEY(invoice_id)
);

CREATE TABLE track_dim(
   track_id INT,
   name VARCHAR(50),
   composer VARCHAR(50),
   milliseconds INT,
   bytes INT,
   unit_price INT,
   media_type VARCHAR(50),
   genre VARCHAR(50),
   album VARCHAR(50),
   artist VARCHAR(50),
   PRIMARY KEY(track_id)
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
   customer_id INT,
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
   support_rep_id INT,
   PRIMARY KEY(customer_id)
);

CREATE TABLE invoice_fact(
   invoice_line_id INT,
   unit_price INT,
   quantity INT,
   customer_id INT,
   track_id INT,
   date_id INT,
   invoice_id INT,
   PRIMARY KEY(invoice_line_id),
   FOREIGN KEY(customer_id) REFERENCES customer_dim(customer_id),
   FOREIGN KEY(track_id) REFERENCES track_dim(track_id),
   FOREIGN KEY(date_id) REFERENCES date_dim(date_id),
   FOREIGN KEY(invoice_id) REFERENCES invoice_dim(invoice_id)
);
