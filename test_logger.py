import json
import logging
import os
from logging.handlers import TimedRotatingFileHandler
restaurant_name = "ABC Restaurant"
name_input = input("Enter Your Name")
main_dict = {}
in_dict = {}
lst = []

logger= logging.getLogger('sample_log')
logger.setLevel(logging.DEBUG)
# handler = logging.FileHandler("logging_2022_07_04.log")
log_handler=TimedRotatingFileHandler("loggerfile",when="s",interval=1)
log_handler.suffix = '%Y_%m_%d.log'
formatter = logging.Formatter('%(asctime)s | %(process)s | %(levelname)s | %(message)s')
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)

def insert_in_json():
    write_file = json.dumps(main_dict, indent=2)
    with open("logger_task_2022_07_04.json", 'w') as file:
        file.write(write_file)


def create_dict():
    inner_lst_dict = {}
    if menu_input == 1:
        inner_lst_dict["menu name"] = "Menu1"
        inner_lst_dict["price"] = "100"
    elif menu_input == 2:
        inner_lst_dict["menu name"] = "Menu2"
        inner_lst_dict["price"] = "200"
    elif menu_input == 3:
        inner_lst_dict["menu name"] = "Menu3"
        inner_lst_dict["price"] = "300"
    elif menu_input == 4:
        inner_lst_dict["menu name"] = "Menu4"
        inner_lst_dict["price"] = "400"
    elif menu_input == 5:
        inner_lst_dict["menu name"] = "Menu5"
        inner_lst_dict["price"] = "500"
    main_dict[restaurant_name] = in_dict
    in_dict[name_input] = lst
    lst.append(inner_lst_dict)
    logger.info('%s %s', name_input,lst)
    print(main_dict)
    insert_in_json()


while True:
    menu_input = None
    try:
        menu_input = int(input("""1. Menu1  -  100\n2. Menu  -  200\n3. Menu  -  300\n4. Menu  -  400\n5.Menu  - 500"""))
    except ValueError:
        print("Please Enter Valid Option")
        logger.warning('Name %s Selected Wrong option', name_input)
        break
    if menu_input == 1 or 2 or 3 or 4 or 5:
        create_dict()

