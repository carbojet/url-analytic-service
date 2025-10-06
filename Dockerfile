# Use an official Node.js image as the base environment
FROM node:20-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy package.json to the container
COPY package*.json ./

# Install dependencies (not needed for this mock, but standard practice)
RUN npm install

# Copy the application source code (now index.js)
COPY index.js .

# Expose the port the app runs on
EXPOSE 3000

# Define the command to run the application using the 'start' script
CMD [ "npm", "start" ]
