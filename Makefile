s3: build_release
	aws s3 cp ./target/release/proxy s3://cp-bucket-harry/bin/
	aws s3 cp ./target/release/light_client s3://cp-bucket-harry/bin/client
build_release:
	cargo build --release
