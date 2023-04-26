import boto3
import gzip
import os
import traceback
import json
import pandas as pd
import time
import psycopg2
import psycopg2.extras

def get_db_con_redshift():
    """
    create database connection and cursor
    """
    user = 'username'
    pwd = 'password'
    host = 'host' 
    port = port
    dbname = 'database_name'

    con = psycopg2.connect(database=dbname, user=user, password=pwd, host=host, port=port)
    cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    con.autocommit = True
    return con, cur

# S3 bucket and folder information
def extract_json():
    con, cur = get_db_con_redshift()
    bucket_name = "s3-bucket"
    s3_folder = "s3-file-path"
    upload_path = "s3-file-path-to-upload"

    # Local directory to save extracted files
    local_dir = "local-path-to-downlod-file"
    local_dir_csv = "local-path-to-save-csv-after-extract"

    key_word = []
    engine_ = []
    created_at_ = []
    processed_at_ = []
    total_time_taken_ = []
    machine_ = []
    link1_ = []
    snippet1_ = []
    displayed_link1_ = []
    position1_ = []
    title1_ = []
    domain1_ = []
    external_id = []
    rich_snippets = []
    country_code = []
    creation_date = []

    # Create S3 client
    s3 = boto3.client("s3")

    # List objects in S3 folder
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)
    # Loop through objects in folder
    for obj in objects["Contents"]:
        # Get object key
        obj_key = obj["Key"]
        # Download object to local file
        try:
            local_file = os.path.join(local_dir, os.path.basename(obj_key))
            s3.download_file(bucket_name, obj_key, local_file)
            # Extract gz file
            with gzip.open(local_file, "rb") as f_in:
                print("Extract gz inprogress")
                with open(os.path.splitext(local_file)[0], "wb") as f_out:
                    f_out.write(f_in.read())
                    print("Extract gz completed")
        except Exception as err:
            traceback.print_exc()
            print(err)
        # break
        
        # Get the list of all files and directories
        # path = "give local path"
        dir_list = os.listdir(local_dir)
        s3_client = boto3.resource('s3')

        for idx,file_name in enumerate(dir_list):
            file_extension = os.path.splitext(file_name)[1]
            print("+++++++++file_name++++++++++",file_name)
            if file_extension == ".json":
                idx=idx+1
                print("Number of files",idx)
                print("+++++++++file_name++++++++++",file_name)
                f = open(f"{local_dir}{file_name}")
                print(f)

                for i, row in enumerate(f.readlines()):
                    try:
                        r = json.loads(row)
                        item = r["Item"]

                        keyword = item["keyword"].get("S")
                        engine = item["search_result"].get("M", {}).get("search_parameters", {}).get("M", {}).get("engine", {}).get("S", {})
                        created_at = item["search_result"].get("M", {}).get("search_metadata", {}).get("M", {}).get("created_at", {}).get("S", {})
                        processed_at = item["search_result"].get("M", {}).get("search_metadata", {}).get("M", {}).get("processed_at", {}).get("S", {})
                        total_time_taken = item["search_result"].get("M", {}).get("search_metadata", {}).get("M", {}).get("total_time_taken", {}).get("S", {})
                        machine = item["search_result"].get("M", {}).get("search_metadata", {}).get("M", {}).get("machine", {}).get("S", {})

                        for i in range(len(item["search_result"].get("M", {}).get("organic_results", {}).get("L", {}))):
                            link1 =  item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get("link", {}).get("S", {})
                            link1_.append(link1)
                            snippet1 =  item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get("snippet", {}).get("S", {})
                            snippet1_.append(snippet1)
                            displayed_link1 =  item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get("displayedLink", {}).get("S", {})
                            displayed_link1_.append(displayed_link1)
                            position1 =  item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get("position", {}).get("N", {})
                            position1_.append(position1)
                            title1 =  item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get("title", {}).get("S", {})
                            title1_.append(title1)
                            domain1 =  item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get("domain", {}).get("S", {})
                            domain1_.append(domain1)
                            key_word.append(keyword)
                            engine_.append(engine)
                            created_at_.append(created_at)
                            processed_at_.append(processed_at)
                            total_time_taken_.append(total_time_taken)
                            machine_.append(machine)
                            rich_snippets.append('Null')
                            country_code.append('Null')
                            creation_date.append('Null')

                    except Exception as err:
                        traceback.print_exc()

                        print(err)
                        print("\n\n\n\n ... missed", i)
                dict = {
                    'search': key_word,
                    'created_at' : created_at_,
                    'position' : position1_,
                    'displayed_url' : displayed_link1_,
                    'name' : title1_,
                    'snippet' : snippet1_,
                    'domain' : domain1_,
                    'search_engine' : engine_,
                    'link' : link1_,
                    'processed_at': processed_at_,
                    'total_time_taken' : total_time_taken_,
                    'machine_': machine_,
                    'external_id': external_id,
                    'rich_snippets': rich_snippets,
                    'country_code': country_code,
                    'creation_date' : creation_date
                }
                df = pd.DataFrame.from_dict(dict, orient='index')
                df = df.transpose()
                file = file_name.split('.')[0]
                df.to_csv(f'{local_dir_csv}{file}.csv', index=False, escapechar='/')
                # ---------- upload to s3 ------------
                try:
                    print("**********starting to upload file in s3************")
                    s3_client.meta.client.upload_file(f'{local_dir_csv}{file}.csv', f'{bucket_name}', f'{upload_path}{file}.csv')
                    print('**********File uploaded to s3*************')
                except Exception as err:
                    print("FIle in gz", err)
                
                 # -------------- copy to redshift ---------------- 
                try:
                    print('******************** COPY IN REDSHIFT INPROGRESS *********************')

                    copy = f"""COPY bing_search_detail_prod_tmp
                    from
                    's3://aws-glue-assets-377041650971-eu-west-1/bing_search_details_prod/bing_serach_detail_prod_csv/{file}.csv'
                    iam_role 'arn:aws:iam::377041650971:role/RedshiftLoaderRole' 
                    region 'eu-west-1'  csv  acceptinvchars AS '-' IGNOREHEADER 1 
                    truncatecolumns  MAXERROR 10 timeformat 'YYYY-MM-DDTHH:MI:SS';"""

                    cur.execute(copy)

                    print('******************** COPY IN REDSHIFT completed *********************')

                except Exception as err:
                    print('Error', err)
                    traceback.print_exc()
                
                f.close()

                # ------- Cleanup ---------------
                
                dict.clear()
                key_word.clear()
                engine_.clear()
                created_at_.clear()
                processed_at_.clear()
                total_time_taken_.clear()
                machine_.clear()
                link1_.clear()
                snippet1_.clear()
                displayed_link1_.clear()
                position1_.clear()
                title1_.clear()
                domain1_.clear()
                external_id.clear()
                rich_snippets.clear()
                country_code.clear()
                creation_date.clear()
            try:
                os.remove(f'{local_dir_csv}{file}.csv')
                os.remove(f'{local_dir}{file_name}')
            except:
                print('File not found')
            else:
                try:
                    os.remove(local_file)
                except:
                    print('file not found')
                continue
            
    con.close()
        
extract_json()