import numpy as np
import pandas as pd
import os
import requests
from tqdm import tqdm
from multiprocessing import Pool
from itertools import repeat

def download_train_data(id_, url, label, dataset_dir):
    if label == 'None':
        print('Id %s has Label None' %(id_))
        return False

    imgdata_dir = os.path.join(dataset_dir, label)
    filename = os.path.join(imgdata_dir, id_+".jpg")

    if os.path.exists(filename):
        print('Image %s already exists. Skipping download.' %(filename))
        return False

    response = requests.get(url)

    if response.status_code != 200:
        print('Connot download image %s. Status code:%d' %(filename, response.status_code))
        return False

    os.makedirs(imgdata_dir, exist_ok=True)

    with open(filename, 'wb') as f:
        f.write(response.content)

    print('Downloaded %s.jpg' %(id_))

    return True

def create_train_dataset(img_ids, img_urls, img_labels, dataset_dir):
    os.makedirs(dataset_dir, exist_ok=True)

    for id_, url, label in tqdm(zip(img_ids, img_urls,   img_labels)):
        download_train_data(id_, url, label, dataset_dir)


def download_test_data(id_, url, dataset_dir):
    if url == 'None':
        print('Id %s has url None' %(id_))
        return False

    filename = os.path.join(dataset_dir, id_+".jpg")

    if os.path.exists(filename):
        print('Image %s already exists. Skipping download.' %(filename))
        return False

    response = requests.get(url)

    if response.status_code != 200:
        print('Connot download image %s. Status code:%d' %(filename, response.status_code))
        return False

    with open(filename, 'wb') as f:
        f.write(response.content)

    print('Downloaded %s.jpg' %(id_))

    return True


def create_test_dataset(img_ids, img_urls, dataset_dir):
    os.makedirs(dataset_dir, exist_ok=True)
    for id_, url in tqdm(zip(img_ids, img_urls)):
        download_test_data(id_, url, dataset_dir)


if __name__=='__main__':
    ### For Train Data
    train = pd.read_csv(os.path.join(os.getcwd(), 'google-landmarks-dataset', 'train.csv')).values
    train_dataset_dir = os.path.join(os.getcwd(), 'dataset', 'train')
    #
    # start = 1210000
    # end = len(train)
    #
    train_img_ids = train[:,0]
    train_img_urls = train[:,1]
    train_img_labels = train[:,2]
    #
    # ### Python is not surported multi-threading because of GIL
    # ### Then, Using the multi-processing
    # os.makedirs(train_dataset_dir, exist_ok=True)
    #
    # with Pool(40) as pool:
    #     pool.starmap(download_train_data,
    #                     zip(
    #                         train_img_ids,
    #                         train_img_urls,
    #                         train_img_labels,
    #                         repeat(train_dataset_dir)
    #                     ))
    #
    # print("\n\nCompleted retrieving images until index %d.\n\n" %end)

    # for label in train_img_labels:
    #     if label == 'None':
    #         continue
    #
    #     result = np.where(train_img_labels == label)
    #
    #     if not str(len(result[0])) == str(len(os.listdir(os.path.join(train_dataset_dir, label)))):
    #         print(label + " ERROR!!")
    #
    # print("Checking complete!")

    ### For Test Data
    # test = pd.read_csv(os.path.join(os.getcwd(), 'google-landmarks-dataset', 'test.csv')).values
    # test_dataset_dir = os.path.join(os.getcwd(), 'dataset', 'test')
    #
    # start = 110000
    # end = 117703
    #
    # test_img_ids = test[start:end,0]
    # test_img_urls = test[start:end,1]
    #
    # os.makedirs(test_dataset_dir, exist_ok=True)
    #
    # with Pool(40) as pool:
    #     pool.starmap(download_test_data,
    #                     zip(
    #                         test_img_ids,
    #                         test_img_urls,
    #                         repeat(test_dataset_dir)
    #                     ))
    #
    # print("\n\nCompleted retrieving images until index %d.\n\n" %end)
