cargo run -- --uri="mongodb+srv://user:pass@server.mongodb.net/?retryWrites=true&w=majority&authSource=admin" --org=dbname --from-date="2022-01-01" --to-date="2022-01-01" --map ./tally_map.csv