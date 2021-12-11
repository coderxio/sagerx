ALTER TABLE datasource.fda_excluded_package
    ADD CONSTRAINT fk_product
        FOREIGN KEY (productid)
            REFERENCES datasource.fda_excluded_product(productid);
