path_file = "C:/PycharmProjects/"
import matplotlib.pyplot as plt


def read_sales_data(file_path):
    file = open(f"{path_file}test_data.txt", "r")  # открываем файл для чтения
    content = file.readlines()
    keys = ["product_name", "quantity", "price", "date"]  # список ключей для словаря
    dict_files = []  # список для записи словарей

    for i in content:  # преобразование каждой итерации в словарь
        values = i.split(", ")
        values[-1] = values[-1].replace("\n", "")
        dicti = dict(zip(keys, values))
        dict_files.append(dicti)
    file.close()
    return dict_files


sales_data = read_sales_data(path_file)


def uniq_product(sales_data):
    uniq_product = {}

    for i in sales_data:  # составление уникального словаря с суммами значений продуктов
        try:
            c = uniq_product[i["product_name"]]
            c += (int(i["quantity"]) * int(i["price"]))
        except KeyError:
            c = (int(i["quantity"]) * int(i["price"]))

        uniq_product[i["product_name"]] = c

    return uniq_product


def total_sales_per_product(sales_data):
    uniq_product_dict = uniq_product(sales_data)

    mx_prod = max(uniq_product_dict, key=uniq_product_dict.get)  # ключ с максимальным значением

    return {mx_prod: uniq_product_dict[mx_prod]}


def uniq_date(sales_data):
    uniq_date = {}

    for i in sales_data:  # составление уникального словаря с суммами значений дат
        try:
            c = uniq_date[i["date"]]
            c += (int(i["quantity"]) * int(i["price"]))
        except KeyError:
            c = (int(i["quantity"]) * int(i["price"]))

        uniq_date[i["date"]] = c

    return uniq_date


def sales_over_time(sales_data):
    uniq_date_dict = uniq_date(sales_data)

    mx_date = max(uniq_date_dict, key=uniq_date_dict.get)  # ключ с максимальным значением

    return {mx_date: uniq_date_dict[mx_date]}


print(total_sales_per_product(sales_data))  # продукт с наибольшей выручкой
print(sales_over_time(sales_data))  # дата с наибольшей выручкой

uniq_product_dict = uniq_product(sales_data)

plt.bar(range(len(uniq_product_dict)), uniq_product_dict.values(), align='center')
plt.xticks(range(len(uniq_product_dict)), uniq_product_dict.keys())

locs, labels = plt.xticks()
plt.setp(labels, rotation=90)

plt.show() #График товаров

uniq_date_dict = uniq_date(sales_data)

plt.bar(range(len(uniq_date_dict)), uniq_date_dict.values(), align='center')
plt.xticks(range(len(uniq_date_dict)), uniq_date_dict.keys())

locs, labels = plt.xticks()
plt.setp(labels, rotation=90)

plt.show() #График дат
