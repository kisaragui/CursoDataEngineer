    select * from (select :id, :idSource, :name, :author, :title, :description, :url, :urlToImage, :publishedAt, :content) as tmp
    where not exists (
        select id from njesusas_coderhouse.articles where id = :id 
    ) LIMIT 1;

