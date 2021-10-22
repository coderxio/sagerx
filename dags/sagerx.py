def create_path(*args):
    """creates and returns folder path object if it does not exist"""
    from pathlib import Path

    p = Path.cwd().joinpath(*args)
    if not p.exists():
        p.mkdir(parents=True)
    return p
