mkdir data1
cd data1
aws s3 cp s3://aws-bigdata-blog/artifacts/amazon-aurora-global-database-multiRegion-webapps/books_data1.zip books_data1.zip
unzip books_data1.zip
aws  s3 cp --recursive . s3://bookreviewstack-bookreviews883228185105useast14969-adhuzvtfx75m/book-review/reviews/ --exclude "*" --include "*json"
cd ..
mkdir data2
cd data2
aws s3 cp s3://aws-bigdata-blog/artifacts/amazon-aurora-global-database-multiRegion-webapps/books_data2.zip books_data2.zip
unzip books_data2.zip
aws  s3 cp --recursive . s3://<region2-s3bucket>/book-review/reviews/ --exclude "*" --include "*json"
cd ..
#cleanup resources
rm -rf data1/ data2/
