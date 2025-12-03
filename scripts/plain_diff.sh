cargo run -r --quiet -- ./data/small-measurements.txt > ./data/result.txt && diff --color ./data/result.txt ./data/small-result-ref.txt
