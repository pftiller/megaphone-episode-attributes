const express = require("express");
const app = express();
const parseRss = require('./megaphone');


const PORT = process.env.PORT || 3001;

parseRss();

app.listen(PORT, (error) => {
      console.log("listening on " + PORT + "...");
  });
