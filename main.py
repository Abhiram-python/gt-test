# import torch
# import boto3
# import os

# # ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
# # ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
# # SECRET_KEY = os.environ["R2_SECRET_KEY"]

# ACCOUNT_ID = "a4ee5e6646eab5d7e69ecaa0f4fe4662"
# ACCESS_KEY = "eb64745db6f8a16b9869b0cc4c76a5f1"
# SECRET_KEY = "ac89a3df48e05c524f585e32053a01df486e752d058b0a1d3274fd3490eb4601"

# s3 = boto3.client(
#     "s3",
#     endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
#     aws_access_key_id=ACCESS_KEY,
#     aws_secret_access_key=SECRET_KEY,
#     region_name="auto",
# )

# l=[1,2,3,4]


# # save locally first
# torch.save(l, "testdata.pt")

# # upload to R2
# s3.upload_file(
#     "testdata.pt",
#     "stx56",                # bucket name
#     "datasets/data.pt"        # object path
# )

# print("uploaded")


print("kokooko")