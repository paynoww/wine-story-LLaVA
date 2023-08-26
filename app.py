from flask import Flask, render_template, request, jsonify
from PIL import Image
import os

app = Flask(__name__)

conversation = []
image_path = None
conversation_active = False

# Placeholder function for processing model responses (replace this with your ML model)
def process_model_response(user_messages):
    # Replace this with your actual ML model's logic
    model_responses = [f"This is the model's response to: {msg}" for msg in user_messages]
    return model_responses

def resize_image(image, max_width, max_height):
    img = Image.open(image)
    img.thumbnail((max_width, max_height))
    return img

@app.route('/', methods=['GET', 'POST'])
def index():
    global image_path, conversation_active
    if request.method == 'POST':
        image = request.files['image']
        if image:
            resized_image = resize_image(image, max_width=800, max_height=600)
            image_filename = os.path.join('static', image.filename)
            resized_image.save(image_filename)
            image_path = image_filename
            conversation.clear()
            conversation_active = True
            conversation.append(f'User: Uploaded image "{image.filename}"')
    
    if conversation_active:
        return render_template('chat.html', image_path=image_path, conversation=conversation, conversation_active=conversation_active)
    else:
        return render_template('index.html')

@app.route('/process_conversation', methods=['POST'])
def process_conversation():
    global conversation_active
    if request.method == 'POST':
        user_messages = request.json['user_messages']
        
        # Process user messages and get model responses
        model_responses = []
        for user_message in user_messages:
            conversation.append(f'User: {user_message}')
            model_response = process_model_response(user_message)
            conversation.append(f'ML Model: {model_response}')
            model_responses.append(model_response)
            
            if conversation_active and model_response == "Conversation finished.":
                conversation_active = False
        
        return jsonify({'model_responses': model_responses})

@app.route('/finish_conversation', methods=['POST'])
def finish_conversation():
    global conversation_active
    conversation_active = False
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
