# Exploration Tests on Redis

Just some scenarios to verify the behaviour of Redis.

** WILL DELETE ALL DATA FROM THE CONNECTED REDIS **

## How to use

* Install Rust via [rustup.rs](https://rustup.rs/)
* Clone this repo
* Start Redis in a Docker container: ```docker run -P redis```
* Run ```docker ps``` to see the exposed port
* Memorize the exposed port of the container
* Enter the cloned repository
* Type ```cargo run --release -- [THE EXPOSED PORT]```
