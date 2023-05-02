link = "https://github.com/NickKletnoi/Interlake/blob/main/CognitiveServices/data/US_20230215_EXAMPLE.pdf"

# note: this will break if a repo/organization or subfolder is named "blob" -- would be ideal to use a fancy regex
# to be more precise here
print(link.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/"))