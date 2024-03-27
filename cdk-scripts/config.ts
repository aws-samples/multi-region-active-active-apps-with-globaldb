//primary site config
type Config = {
    S3ReviewsPendingPrefix:string
    S3ReviewsCompletedPrefix:string
    DBName:string
}

export const config: Config = {
    S3ReviewsPendingPrefix:"book-review/reviews/",
    S3ReviewsCompletedPrefix:"book-review/completed/",
    DBName:"reviews_db"
}
