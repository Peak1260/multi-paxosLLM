import google.generativeai as genai

genai.configure(api_key="AIzaSyC64zw3CDFPuK1IJsyB_PyGA355XnmT2zw")
model = genai.GenerativeModel("gemini-1.5-flash")
chat = model.start_chat()

context = "Query: Can you name three mammals? Answer: Dog, cat, elephant " #this is the string in our dictionary
prompt = "Can you name the third animal?" #this will be the question we put in our command

# response = chat.send_message(prompt)
# print(response.text)


    
response = model.generate_content(context + prompt)
response = "Answer: " + response.text
#print("response: ", response)
print(response)