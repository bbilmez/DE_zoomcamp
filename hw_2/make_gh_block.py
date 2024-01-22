from prefect.filesystems import GitHub

# alternative to creating GitHub block in the UI

gh_block = GitHub(
    name="de-zoom", repository="https://github.com/bbilmez/DE_zoomcamp"
)

gh_block.save("de-zoom", overwrite=True)