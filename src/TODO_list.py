TODO_list = {
    "aggiungere metodo di costruzione filter_location_id_rest_request",
    "testare DB_bronze_builder",
    "add fKey method"
}

def Print_TODO_list():

    for i in TODO_list:
        print(f"{i+1}- {TODO_list[i]}")
        n+=1

if __name__ == "__main__":
    Print_TODO_list()