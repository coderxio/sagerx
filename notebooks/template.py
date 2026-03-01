import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    import sqlalchemy

    DATABASE_URL = os.environ.get("DATABASE_URL")
    sagerx = sqlalchemy.create_engine(DATABASE_URL)
    return (sagerx,)


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo, sagerx):
    _df = mo.sql(
        f"""
        select
        	*
        from sagerx_dev.stg_fda_ndc__ndcs
        limit 100

        """,
        engine=sagerx
    )
    return


if __name__ == "__main__":
    app.run()
