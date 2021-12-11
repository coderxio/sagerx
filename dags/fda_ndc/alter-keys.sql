ALTER TABLE datasource.fda_ndc_package
    ADD CONSTRAINT fk_product
        FOREIGN KEY (productid)
            REFERENCES datasource.fda_ndc_product(productid);
