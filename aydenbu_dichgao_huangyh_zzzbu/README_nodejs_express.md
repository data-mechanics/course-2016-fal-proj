
In this project we use node.js with express to create a simple web app to visualize our data through D3.

The codes that are in this part are server.js and 2 files in a sub_dictionary views(bubble.ejs, histagram.ejs)
Put server.js in the dictionary you wanna run them and create a folder named views and put bubble.ejs, histagram.ejs inside of it.
To run our code, you have to first install node.js:
https://nodejs.org/en/
Choose the 6.9.2 version. Because the npm may not work with the 7.2.1 version.
You can check the node.js version and npm version though:
$ node -v
$ npm -v
You should have both of them after you install node.js.
After that init the npm in the Dictionay that you want to run the codes.
To init use :
$ npm init
Then, install express:
$ npm install express --save
install body-parser for read data:
$ npm install body-parser --save
install mongdb package for connection with mongodb:
npm install mongodb --save
install ejs as our template engine :
$ npm install ejs --save

now you should done with setting up.
To run the code:
node server.js
and you can see the web app through local host 3000
