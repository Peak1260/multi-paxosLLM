# Description: Gemini ouputs a warning when Allan runs it on his computer. 
# This is code that Allan uses to test the Gemini model.

import os
# import logging
import google.generativeai as genai
# from grpc import graceful_shutdown
# import atexit

# Set environment variables to suppress gRPC warnings
os.environ["GRPC_VERBOSITY"] = "NONE"
# os.environ["GRPC_TRACE"] = ""

# Suppress gRPC warnings
# logging.getLogger("absl").setLevel(logging.WARNING)

# Register graceful shutdown
# atexit.register(graceful_shutdown, timeout=5)

genai.configure(api_key="AIzaSyC64zw3CDFPuK1IJsyB_PyGA355XnmT2zw")
model = genai.GenerativeModel("gemini-1.5-flash")
chat = model.start_chat()

context = "Query: Can you name three mammals? Answer: Dog, cat, elephant " #this is the string in our dictionary
prompt = "Can you name the third animal?" #this will be the question we put in our command

response = model.generate_content(context + prompt)
response = "Answer: " + response.text
print(response)
