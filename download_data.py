from azure.storage.blob import BlobClient


##GET AND DOWNLOAD SALES DATA
storage_account_name = 'https://orderstg.blob.core.windows.net/'
storage_account_access_key = "?sv=2019-12-12&ss=bfqt&srt=sco&sp=rlx&se=2030-07-28T18:45:41Z&st=2020-07-27T10:45:41Z&spr=https&sig=cJncLH0UHtfEK1txVC2BNCAwJqvcBrAt5QS2XeL9bUE%3D"

blob_container = 'ordersdow'

blob_names = ["00.csv","01.csv","02.csv", "04.csv","05.csv"]
for blob_name_f in blob_names:
    blob = BlobClient(account_url=storage_account_name,
                      container_name=blob_container,
                      blob_name=blob_name_f,
                      credential=storage_account_access_key)
    with open(blob_name_f, "wb") as f:
        data = blob.download_blob()
        data.readinto(f)
