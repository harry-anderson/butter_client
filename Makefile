s3: build_release
	aws s3 cp ./target/release/proxy s3://cp-bucket-harry/bin/
build_release:
	cargo build --release
