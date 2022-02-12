const express = require("express");
const app = express();
const fetchEpisodes = require('./megaphone');


const PORT = process.env.PORT || 3001;

fetchEpisodes();

app.listen(PORT, (error) => {
      console.log("listening on " + PORT + "...");
  });
