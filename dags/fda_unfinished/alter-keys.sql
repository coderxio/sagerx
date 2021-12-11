ALTER TABLE datasource.fda_unfinished_package
    ADD CONSTRAINT fk_product
        FOREIGN KEY (productid)
            REFERENCES datasource.fda_unfinished_product(productid);
