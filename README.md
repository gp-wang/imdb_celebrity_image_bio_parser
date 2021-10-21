# IMDB to MSID Celebrity Image Parser And Matcher

This project find the top 5k celebrity names ranked in imdb. It also uses the ms1m dataset (The Microsoft dataset for human face recognition) and see if these found celebrities already have photos in the ms1m dataset. If not, it will connect to azure image API and find facial photos for this missing celebrities names. 
This is a sub project for Celebscope, which is an application to do facial recognition for the top 5k movie stars ranked on IMDB.


## Usage

./find_celebs_in_imdb_top_list_but_missing_in_ms1m_datasets.py 

results:
    imdb top 5k celeb name 
    parsed from imdb
    
    
link
    matched imdb_id (nm...) to ms1m_id(m.xxxxxx)
    
    
