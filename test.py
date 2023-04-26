import json
import traceback
import csv

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
f = open('k.json', 'r')

for i, row in enumerate(f.readlines()):
    try:
        r = json.loads(row)
        item = r["Item"]

        keyword = item["keyword"].get("S")
        engine = item["search_result"].get("M", {}).get("search_parameters", {}).get("M", {}).get("engine", {}).get("S",
                                                                                                                    {})
        created_at = item["search_result"].get("M", {}).get("search_metadata", {}).get("M", {}).get("created_at",
                                                                                                    {}).get("S", {})
        processed_at = item["search_result"].get("M", {}).get("search_metadata", {}).get("M", {}).get("processed_at",
                                                                                                      {}).get("S", {})
        total_time_taken = item["search_result"].get("M", {}).get("search_metadata", {}).get("M", {}).get(
            "total_time_taken", {}).get("S", {})
        machine = item["search_result"].get("M", {}).get("search_metadata", {}).get("M", {}).get("machine", {}).get("S",
                                                                                                                    {})

        for i in range(len(item["search_result"].get("M", {}).get("organic_results", {}).get("L", {}))):
            link1 = item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get(
                "link", {}).get("S", {})
            link1_.append(link1)
            snippet1 = item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get(
                "snippet", {}).get("S", {})
            snippet1_.append(snippet1)
            displayed_link1 = item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M",                                                                                                    {}).get(
                "displayedLink", {}).get("S", {})
            displayed_link1_.append(displayed_link1)
            position1 = item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get(
                "position", {}).get("N", {})
            position1_.append(position1)
            title1 = item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get(
                "title", {}).get("S", {})
            title1_.append(title1)
            domain1 = item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})[i].get("M", {}).get(
                "domain", {}).get("S", {})
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
            
            
            
            
            
# print(len(item["search_result"].get("M", {}).get("organic_results", {}).get("L", {})))


    except Exception as err:
        traceback.print_exc()

        print(err)
        print("\n\n\n\n ... missed", i)

    dict1 = {
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


    import csv
    head = list(dict1.keys())
    with open('Demo.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = head)
        writer.writeheader()
        writer.writerow(dict1)

    csvfile.close()
    f.close()
    
    key_word.clear()
    created_at_.clear()
    position1_.clear()
    displayed_link1_.clear()
    title1_.clear()
    snippet1_.clear()
    domain1_.clear()
    engine_.clear()
    link1_.clear()
    processed_at_.clear()
    total_time_taken_.clear()
    machine_.clear()
    external_id.clear()
    rich_snippets.clear()
    country_code.clear()
    creation_date.clear()


    