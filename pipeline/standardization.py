#!/usr/bin/env python3
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import uuid
import re
import sys
import requests
import json
import traceback
from utils.db_connector import get_db_engine

# Load environment variables
load_dotenv()

# ==============================================================================
#  1. USER PROVIDED MAPPING & LOGIC
# ==============================================================================

NAMESPACE_UUID = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

PARENT_CHILD_MAPPING = {
    # --- VEGETABLES & FRUITS (Strictly Separated) ---
    "Red Onion A": ["Red Onion A", "Red Onion Grade A", "Red Onion Grade A Restaurant q", "Red Onion Grade A Restaurant quality", "Red Onion", "Red Onion Qelafo"],
    "Red Onion B": ["Red Onion B", "Red Onion Grade B"],
    "Red Onion C": ["Red Onion C", "Red Onion Grade C", "Pilled Red onion"],
    "Red Onion D": ["Red Onion D"],
    "Small Red Onion": ["Small Red Onion"],
    "Red Onion Elfora": ["Red Onion Elfora", "Redonion Elfora"],
    "Red Onion (ሃበሻ)": ["Red Onion (ሃበሻ)", "Red onion ( ሃበሻ ) "],
    "White Onion A": ["White Onion A"],
    "White Onion B": ["White Onion B"],
    "White Onion C": ["White Onion C"],

    "Carrot": ["Carrot"],
    "Carrot B": ["Carrot B"],
    "Small Size Carrot": ["Small Size Carrot", "Small Carrot", "Small size Carrot"],

    "Tomato A": ["Tomato A", "Tomatoes Grade A", "Tomato", "Tomato Restaurant Quality ", "Tomatoes A", "Tomato Grade A"],
    "Tomato Ripe": ["Tomato/ Ripe/ Small size /", "Tomato Ripe"],
    "Tomato B": ["Tomato B", "Tomatoes B", "Tomatoes Grade B"],
    "Tomato Roma": ["Tomato Roma", "Tomato Beef"],

    "Potato": ["Potato", "Potatoes", "Potatoes Restaurant quality", "Potatoes Restaurant Quality"],
    "Potato Chips": ["Potato for Chips", "Potato Chips", "Potatoes Chips"],
    "Potato Grade C": ["Potato Grade C", "Potato C"],
    "Small Size Potato": ["Small size Potato", "Small Size Potato "],

    "Chili Green": ["Chili Green", "Chilly Green"],
    "Chilly Green (Starta)": ["Chilly Green (Starta)", "Green Chili", "Green Chilli", "Green Chili (ስታርታ)", "Chilly Green (Starter)"],
    "Chilly Green Elfora": ["Chilly Green Elfora", "Chilly Green (Elfora)"],
    "Green Pepper": ["Green Pepper", "green pepper"],
    "Indian Chilly": ["Indian chilly", "indian chilly"],
    "Chilly Short": ["Chilly short"],

    "Beetroot": ["Beetroot", "beetroot", "Beet root"],
    "Beetroot Small Size": ["Beetroot Small Size"],
    "Small & Big Size Beetroot": ["Small & Big Size Beetroot"],

    "White Cabbage": ["White Cabbage", "White Cabbage B", "White Cabbage (Large)", "White Cabbage (Small)", "White Cabbage (medium)", "whitecabbage", "White cabbage", "Cabbage"],
    "Habesha Cabbage": ["Habesha Cabbage", "Habesha cabbage", "Gurage cabbage"],

    "Avocado": ["Avocado", "Avocado A", "Avocado OG", "Avocado Shekaraw", "Local Avocado"],
    "Avocado B": ["Avocado B"],
    "Avocado Ripe": ["Ripe Avocado", "Avocado Ripe"],

    "Strawberry": ["Strawberry"],
    "Papaya": ["Papaya"],
    "Papaya B": ["Papaya B"],
    "Papaya Oversize": ["Papaya Oversize"],

    "Cucumber": ["Cucumber"],
    "Garlic": ["Garlic", "Garlic B", "garlic"],
    "Garlic China": ["Garlic China"],
    "Garlic Local": ["Garlic Local"],
    "Garlic Small Size": ["Garlic Small Size", "Garlic (የተፈለፈለ)", "Hatched Garlic"],

    "Ginger": ["Ginger", "Ginger Local", "Ginger Thai"],
    "Pineapple": ["Pineapple", "pineapple"],
    "Pineapple B": ["Pineapple B"],
    "Mango Apple": ["Mango Apple", "Apple Mango"],
    "Lemon": ["Lemon", "lemon", "Lomen", "Lemon 250g", "Lime"],
    "Orange Valencia": ["Orange", "Valencia Orange", "orange Valencia", "Valencia Orange"],
    "Orange Yerer": ["Orange Yerer", "Yerer Orange", "Orange Yarer "],
    "Orange Pineapple": ["Orange Pineapple", "Orange Pineapple "],
    "Apple": ["Apple", "Appel"],
    "Corn": ["Corn"],
    "Mango": ["Mango", "Ye Habeshaa Mango", "Habesha Mango", "Ye Habesha Mango"],
    "Green Beans": ["Fossolia", "Green Beans", "Grean Beans", "Green bean", "Fossolia (250g)"],

    "Lettuce": ["Lettuce", "Chinese lettuce", "Iceberg Salad", "Iceberg", "Lolo rosso"],
    "Spinach": ["spinach", "Spinach", "Swiss chard", "Kale"],
    "Celery": ["Celery", "Parsley", "Leek", "Coriander"],
    "Salad": ["Salad"],
    "Broccoli": ["Broccoli", "Broccolis"],
    "Cauliflower": ["Cauliflower"],

    "Sweet Potato": ["Sweet Potatoes", "sweet potato", "Sweet Potato"],
    "Banana": ["Banana", "Banana/ Raw "],
    "Eggplant": ["Eggplant", "Egg plant", "eggplant"],
    "Zucchini": ["Zucchini", "Courgette", "Courgetti"],
    "Pumpkin": ["Pumpkin"],
    "Squash": ["squash"],
    "Watermelon": ["Watermelon"],

    # --- DACHI PRODUCTS ---
    "Dachi Vimto": ["dachi vimto", "Dachi Vimto"],
    "Dachi Vinegar": ["dachi vinegar"],
    "Dachi Ketchup": ["dachi ketchiup", "Dachi Ketchup", "Dachi Ketchup ", "Dachi Kethup"],
    "Dawedo Pepper": ["Dawed pepper ", "Dawedo Pepper "],

    # --- TEFF ---
    "Shega Red Teff": ["Shega Red Teff"],
    "Shega White Teff": ["Shega White Teff", "Shega Teff"],
    "Ture Red Teff": ["Ture Red Teff"],
    "Ture White Teff": ["Ture White Teff"],

    # --- GRAINS, PULSES & FLOUR ---
    "Ater Kik": ["Ater Kik"],
    "Chickpea": ["Chickpea", "Shembera"],
    "Lentils": ["lentils", "difen misir", "Difen miser (Imported)", "Sambusa Miser", "Lentils (imported)"],
    "Aja Kinche": ["Aja kinche", "Aja Kinche"],
    "Barley Kinche": ["Yegebes Kinche", "kinche yegebis", "Kinche Yegebs"],
    "AL-Hinan Flour": ["AL-Hinan Flour", "AL-Hinan flour", "AL-Hinan Food Complex", "al Hilal flour"],
    "Bethel Flour": ["bethel flour", "Bethel Flour"],
    "Wakene Flour": ["wakene flour", "Wakene flour", "Wakene 1st Level Flour"],
    "DK Geda Flour": ["DK GEDA FLOUR", "Dh geda flour"],

    # --- OILS ---
    "Tena Cooking Oil": ["Tena Cooking oil", "Tena cooking oil 1L", "Tena cooking oil 5L"],
    "Aluu Sunflower Oil": ["Aluu Pure Sunflower Oil", "Aluu Sunflower Oil"],
    "Omar Sunflower Oil": ["Omaar sunflower oil", "Omar Sunflower Oil", "omaar ghee(500g)", "Omaar Pure Vegetable Ghee"],
    "Dania Oil": ["Dania oils", "Dania Sunflower Cooking Oil"],
    "Inci Oil": ["inci oil", "Inci sunflower oil"],
    "Bahja Sunflower Oil": ["Bahja Sunflower Oil"],
    "Bizce Sunflower Oil": ["Bizce Sunflower Oil"],
    "Hilwa Sunflower Oil": ["Hilwa Sunflower Oil"],

    # --- WATER ---
    "Victory Water": ["Victory water 1L", "Victory water 2L", "Victory Natural Water/Pack", "Victory Water ", "victory purified natural water"],
    "Yes Water": ["Yes Water", "Yes water 0.33lt", "Yes Water 1L", "Yes water 2L", "Yes water 500ml", "Yes Water (2L)", "Yes Water (Pack)"],
    "Gold Water": ["Gold Water"],
    "Top Bottled Water": ["Top Bottled Water"],

    # --- TEA & COFFEE ---
    "Addis Tea": ["Addis tea", "Addis Tea", "Addis Tea Bag"],
    "Black Lion Tea": ["Black lion tea", "Black Lion (40g)", "Black Lion Tea (80g)"],
    "Wush Wush Tea": ["wush wush tea", "Wush Wush Tea"],
    "Simba Tea": ["Simba tea bag", "Simaba tea bag tyhme", "Simba Tea Bag", "Simba Tea Bag (Thyme)"],
    "Girum Cinnamon Tea Bag": ["Girum Cinamon Tea Bag", "Girum Cinnamon Tea Bag "],
    "AMG Coffee": ["AMG coffee", "Amg Coffee 50g", "AMG Coffee (Grinded)", "AMG Coffee (Roasted)", "AMG Grinded Coffee"],
    "Mawi Coffee": ["Mawi Coffee ", "Mawi coffee "],
    "Akkoo Coffee": ["Akkoo Coffee"],

    # --- DAIRY & HONEY ---
    "Cheese": ["Cheese", "አይብ", "Ethiopian Cheese", "Cheese 500g"],
    "Cheese Package": ["Cheese Package", "አይብ ጥቅል"],
    "Butter": ["Butter", "Table Butter", "Shola Table butter", "Tycoon Table Butter", "sheno lega ghee", "Sheno Lega Ghee"],
    "Bonga Honey": ["Bonga Honey", "Bonga Mar"],
    "Ami White Honey": ["ami white honey", "AMI White Honey"],
    "Tesfaye Peanut Butter": ["Tesfaye Peanut Butter", "Tesfaye Peanut"],
    "Zagol Yoghurt": ["Zagol Yoghurt", "Zagol yoghurt"],
    "Habesha Mitin Shiro": ["Habesha Mitin Shiro", "Habsha Mitin Shiro"],

    # --- CLEANING & HYGIENE ---
    "CK Powdered Soap": ["CK laundary soap", "CK Powdered Soap", "Ck Powder Soap", "Ck powdered soap", "CK Laundry Soap", "CK Powdered Soap (40g)", "ck powderd soap 150g", "ck powderd soap 1kg", "ck powderd soap 40g", "ck powderd soap 500g", "ck powderd soap 5kg"],
    "Cloud Bleach": ["cloud bleach 5L", "Cloud Bleach", "Cloud Bleach / 1L", "Cloud Bleach / 5L"],
    "Sunny Bleach": ["Sunny Bleach", "Sunny Bleach / 5L", "Sunny Bleach 5 Liter", "Sunny Bleach / 800ML"],
    "YALI Bleach": ["YALI Bleach 5lit ", "Yali Bleach 5L", "YALI Bleach 1 Lit", "YALI Bleach 350ml"],
    "YALI Multi Purpose 2lit": ["YALI Multi Purpose 2lit ", "Yali Multi purpose 2lit", "YALI Multi Purpose 1lit"],
    "YALI Multi Purpose 5lit": ["YALI Multi Purpose 5lit", "Yali Multi purpose 5lit"],
    "Ajax Soap": ["ajax soap", "Ajax (Large)"],
    "Duru Soap": ["DURU soap", "Duru Soap", "Duru Soap (180g)"],
    "Lifebuoy Soap": ["life bouy 70g", "Life Buoy (Big)", "Life Buoy ( Big)", "Lifebuoy 70g", "Lifebuoy antibacterial bar soap", "lifebuoy(70g)"],
    "Yeah laundry bar soap": ["YEAH Bar Soap", "Yeah laundry bar soap"],
    "Roll Detergent powder": ["Rol Bio Laundry powder ", "Roll Detergent powder"],
    "Happy Tissue": ["Happy soft", "happy toilet paper", "Happy Toilet Tissue", "Happy Toilet Paper"],
    "Bravo Tissue": ["bravo table tissue", "bravo toilet paper", "Bravo Table Tissue", "Bravo Toilet Paper"],
    "Tulip Toilet Paper": ["Tulip Toilet Paper", "Tulip soft "],

    # --- MEAT & POULTRY ---
    "Lamb": ["Lamb", "Lamp", "የበግ ስጋ", "ጠቦት የበግ ስጋ", "Regular Mutton Package", "Sheep package", "Mutton"],
    "Goat Meat": ["Goat Meat", "የፍየል ስጋ", "Special Goat Package", "Regular Goat Package", "ሙክት የፍየል ስጋ"],
    "Chicken Package": ["Chicken", "Chicken Groceries", "Regular Chicken Package", "Special Chicken Package", "BGS Foreign Chicken", "12 Piece Chicken", "Chicken Package", "Habesha Chicken", "Habesha Chicken Package"],

    # --- PACKAGES ---
    "Difo Package": ["Difo Package", "Difo package ", "ድፎ ጥቅል", "ዳቦ ጥቅል", "Defo Package", "Special Defo package"],
    "Qulet Package": ["Qulet Package", "Qulet package ", "ቁሌት ጥቅል ", "ቁሌት ጥቅል", "Special qulet package"],
    "Kukulu Package": ["Kukulu Package", "Kukulu package", "Special Kululu Package"],

    # --- PASTA & NOODLES ---
    "Indomie Vegetable Noodles": ["Indomie Vegetable Noodles ", "Indomie Vegie Noodles "],
    "MIA Pasta": ["MIA Pasta", "Mia Pasta"],

    # --- BISCUITS ---
    "Moon Vanilla Biscuit": ["Moon Vanilla Biscuit ", "Moon vanilla biscut "],

    # --- STATIONERY (Separated by Brand/Item) ---
    "Afro EIIDE Exercise Book": ["Afro Eiide Exercise book", "Afro EIIDE Exercise Book", "EIIDE Exercise Book (12 pieces)"],
    "Radical Exercise Book": ["Radical Exercise Book/ Pack", "radical exercise book", "Radical Exercise Book/50p"],
    "Buna Pen": ["buna pen"],
    "Buna Pencil": ["Buna pencil"],
    "Pencil": ["Pencil"],
    "Tooth Brush": ["Tooth brush"],

    # --- CLOTHING (Separated by Type) ---
    "Casual T-Shirt": ["Casual T-Shirt"],
    "Cotton T-Shirt": ["Cotton T-Shirt", "Cotton T-shirt"],
    "Printed T-Shirt": ["Printed T-shirt", "printing T-shirt"],
    "T-Shirt": ["T-shirt"],

    # --- BAGS ---
    "Cross-body Bag": ["Cross-body Bag", "Crossbody Bag"],

    # --- BABY PRODUCTS ---
    "ABC Diaper": ["ABC Diaper", "ABC Daiper"],

    # --- KITS (Separated by Team) ---
    "Arsenal Kit": ["Arsenal 2024/2025 Kit"],
    "Liverpool Kit": ["Liverpool 2024/2025 Kit"],
    "Manchester City Kit": ["Manchester City 2024/2025 Kit"],
    "Manchester United Kit": ["Manchester United 2024/2025 Kit"],

    # --- NEW ADDITIONS (USER REQUESTED) ---
    "Pepper": ["Red Pepper", "Yellow Pepper"],
    # Note: User requested White Onions to be mapped under Garlic


    # --- NEW PARENTS (SELF-MAPPED) ---
    "Corn Kinche": ["Corn kinche"],
    "Corn Starch": ["Corn starch"],
    "Rice": ["Rice", "Dania rice", "Dania Basmati Rice"],
    "Rosmery": ["Rosmery"],
    "Taza Oats": ["Taza oats"],
    "Cerifam Fruit": ["Cerifam fruit", "Cerifam Normal"],
    "Cerifam Vegetable": ["Cerifam vegetable"],
    "Bessobela": ["Bessobela (holy basil)", "Holy Basil, በሶ ብላ"],
    "Digis Table Salt": ["Digis table salt"],
    "Flax Seed": ["Flax seed"],
    "Knorr": ["Knorr"],
    "Mekelesha": ["Mekelesha", "Mekelsha"],
    "My Kishin Mekelsha 2.5g": ["My kishin mekelsha 2.5g"],
    "My Kishin Mekelsha 2g": ["My kishin mekelsha 2g", "My Kishen Mekelesha (2g)"],
    "Mint": ["Mint"],
    "Pure Salt": ["Pure salt"],
    "Safa Tomato Paste": ["Safa tomato paste", "Safa Tomato Paste 400g", "Hilal Tomato Paste"],
    "Salasa Tomato Paste": ["Salasa tomato paste"],
    "Simba Tomato Paste": ["Simba tomato paste"],
    "Sugar": ["Sugar", "Sugar (1kg)"],
    "Take Away Box": ["Take away box"],
    "Theday Strawberry Jam": ["Theday strawberry jam"],
    "Egg": ["Egg"],
    "Almi Dabo Kolo": ["Almi dabo kolo"],
    "Almi Kolo": ["Almi kolo"],
    "Bourbon Cream Biscuit": ["Bourbon cream biscuit"],
    "Butter Cups Biscuit": ["Butter cups biscuit", "Butter Cups Fasting Biscuit", "Beka ButterCups Fasting Biscut"],
    "Cinni Krunches Biscuit": ["Cinni Krunches biscuit", "Cinni Krunches Fasting Biscuit", "Beka Cinni Fasting Biscuit"],
    "Coffee Late Cream Biscuit": ["Coffee late cream biscuit", "Coffee Latte Cream Biscuit"],
    "Julet Biscut": ["Julet biscut"],
    "Knick Knack Biscut": ["Knick knack biscut", "Knick Knack"],
    "Loli Chips": ["Loli chips"],
    "Moon Cookies": ["Moon cookies"],
    "Moon Cookies All Flavors": ["Moon cookies all flavors"],
    "Moon Cookies Cinnamon": ["Moon cookies cinnamon"],
    "Moon Cookies Coffee": ["Moon cookies coffee"],
    "Moon Cookies Vanilla": ["Moon cookies vanilla"],
    "Moya Biscut": ["Moya Biscut", "Moya Biscuit"],
    "Nib Bar Chocolate": ["Nib Bar chocolate"],
    "Nib Chocolate 350 Gram": ["Nib Chocolate 350 gram"],
    "Nib Chocolate Mini": ["Nib chocolate mini", "Nib Mini Chocolate (5pcs)"],
    "Ok Wafers": ["Ok wafers", "Ok Cream Wafer"],
    "Pop Corn": ["Pop corn"],
    "Sun Chips": ["Sun chips", "Sun Chips (30g)"],
    "Sunflower Seeds": ["Sunflower seeds", "Sunflower Seeds(Suf)", "sunflower seeds"],
    "Yummy Crunchy": ["Yummy crunchy"],
    "Yummy Crunchy & Rasins 500g": ["Yummy crunchy & rasins 500g", "Yummy Crunchy & Raisins"],
    "Zion Biscut": ["Zion biscut", "Zion biscuit"],
    "Macaroni": ["Macaroni", "Booez Macaroni", "Ok Macaroni"],
    "Ok Macaroni 500g": ["Ok Macaroni (500g)"],
    "Rosa Pasta": ["Rosa Pasta", "Booez Pasta", "Mawel pasta", "Alvima Pasta"],
    "Baby Diaper": ["B&B Baby diaper no1 to 5", "B&B Baby Diaper"],
    "Baby Wipes": ["B&b baby wipes", "ABC baby wipes", "Good Baby Wipes"],
    "Bella Sanitary Pad": ["Bella sanitary pad", "Eve sanitary pad"],
    "Nigist Sanitary Pad": ["Nigist sanitary pad"],
    "Fafa Baby Food": ["Fafa baby food"],
    "Avocado Hair Oil": ["Avo avocado hair oil"],
    "Cloud Hand Wash": ["Cloud hand wash 5l"],
    "Cloud Multipurpose": ["Cloud multipurpose", "Cloud Multi Purpose", "Cloud Multi Purpose/ 1L", "Cloud Multi Purpose/ 2L", "Cloud Multi Purpose/ 5L", "Cloud multi purpose 5l"],
    "Cloud Window Cleaner": ["Cloud Window Cleaner"],
    "Crown Detergent": ["Crown powdered detergent (30g)", "Crown Powder Detergent (30g)"],
    "Crown Soap": ["Crown powdered soap (180g)", "Crown Laundry Soap"],
    "Dabur Herbal Toothpaste 150g": ["Dabur herbal tooth paste 150g"],
    "Dabur Herbal Toothpaste 30g": ["Dabur herbal tooth paste 30g"],
    "Dabur Herbal Toothpaste 50g": ["Dabur herbal tooth paste 50g", "Dabur Herbal Tooth Paste", "Dabur Tooth Paste (50g)"],
    "Diana Soap": ["Diana soap 80g", "Diana Orange Toilet Soap (80gm)", "Diana Toilet Soap (20gm)", "Diana Toilet Soap (25gm)"],
    "Ipas Soap": ["Ipas soap"],
    "Kono Soap": ["Kono soap"],
    "Lux Soap": ["Lux 70g"],
    "Rose Laundry Soap": ["Rose laundry soap"],
    "Sunsilk Shampoo": ["Sunsilk shampoo 350ml"],
    "Varika Shampoo": ["Varika shampoo"],
    "Vaseline": ["Vasline"],
    "Vaseline Coconut Lotion 200ml": ["Vasline cocnut lotion 200ml"],
    "Vaseline Dry Skin 200ml": ["Vasline dry skin 200ml"],
    "Vatika Hair Oil": ["Vatika hair oil"],
    "Afar Salt": ["Afar Salt"],
    "Custard Pie": ["Custard Pie"],
    "Dish Wash": ["Dish Wash"],
    "Kakao Powder": ["Kakao Powder"],
    "Minced Meat": ["Minced Meat"],
    "Quanta": ["Quanta", "ET Quanta"],
    "Yummy Peanut Butter": ["Yummy Peanut Butter", "Yummy Crunchy Peanut Butter", "Yummy Smooth Peanut Butter"],
    "Pumpkin": ["Pumpkin"],
    "Squash": ["squash", "Squash"],
    "Red Pepper": ["Red Pepper"],
    "Yellow Pepper": ["Yellow Pepper"],
    "Sunny Softener": ["Sunny Softener", "Sunny softener"],
    "YALI Powder Cleaner": ["YALI Powder cleaner"],
    "Ok Noodles": ["Ok Vegetable Noodle"],
    "Sunny Body Jel": ["Sunny Body Jel"],
    "Shea Butter": ["Dainty natural shea butter"],
    "555 Soap": ["555 Laundry soap"],
    "Sunny Shampoo": ["Sunny Shampoo"],
    "Granola": ["Loose granola (Cereal)"],
    "Small Bites Pack": ["Small bites pack"],

    # --- ADDITIONAL MISSING PRODUCTS ---
    "Sosi": ["Sosi"],
    "Indomie": ["indomie (110g)"],

    "Bright Detergent": ["bright detergent"],

    # --- CLOTHING (Additional) ---
    "Cardigan": ["Cardigan", " Cardigan"],
    "Casual Sweater": ["Casual Sweater"],
    "Hoodie": ["Hoodie"],
    "Leggings": ["Leggings"],
    "Football Jersey": ["Football Jersey"],
    "Rib Stylish T-Shirt": ["Rib Stylish T-shirt"],
    "Kids Pajama Set": ["Kids Pajama Set"],
    "Two Piece Kids Pajama Set": ["Two Piece Kids Pajama Set"],
    "Turtle Neck": ["Turtle Neck"],
    "Knitted Blanket": ["Knitted blanket(Medium)"],

    # --- BAGS & ACCESSORIES (Additional) ---
    "Belt Bag": ["Belt Bag"],
    "Canvas Bag": ["Canvas Bag"],
    "Tote Bag": ["Tote Bag", "Tote bag"],
    "Alem Card Holder": ["Alem Card Holder"],
    "Wallet": ["Wallet"],
    "Leather Hand Bags": ["Leather Hand Bags"],
    "Kabana Toiletry Bags": ["Kabana Toiletry Bags"],
    "Toiletry Hand Bags": ["Toiletry Hand Bags"],

    # --- HAIR & BODY PRODUCTS ---
    "Paraffin Hair Oil": ["Paraffin hair oil"],
    "Avo Carrot Hair Oil": ["Avo carrot", "Avo carrot hair oil"],
    "Snail Hair Oil": ["Snail hair oil"],
    "Taflen Paraffin Hair Oil": ["Taflen Paraffin Hair Oil"],
    "Zalash Hair Oil": ["Zalash hair oil", "Zalash hair oil"],
    "Zenith Hair Oil": ["Zenith hair oil"],
    "Vatika Almond Hair Oil": ["Vatika Almond Hair Oil"],
    "Vatika Coconut Hair Oil": ["Vatika Coconut Hair Oil"],
    "Vatika Black Seed Hair Oil": ["Vatika Hail Oil (Black Seed)"],
    "Vatika Garlic Hair Oil": ["Vatika Hairl Oil (Garlic)"],
    "Vatika Olive Hair Oil": ["Vatika Olive Hair Oil", "Vatika Olive Hair Oil"],
    "Vatika Henna Shampoo": ["Vatika Henna Shampoo"],
    "Vatika Lemon Shampoo": ["Vatika Lemon Shampoo"],
    "Vatika Shampoo": ["Vatika Shampoo"],
    "Dainty Natural Shea Butter": ["Dainty natural shea butter"],
    "Dexe Black Hair Shampoo": ["Dexe Black Hair shampoo"],
    "Organza Shampoo": ["Organza Shampoo"],
    "Aloha Conditioner": ["Aloha conditioner"],
    "Sunsilk Conditioner": ["Sunsilk conditioner", "Sunsilk conditioner Big"],
    "Sunsilk Coconut Shampoo": ["Sunsilk coconut Shampoo"],
    "Sunsilk Shampoo Big": ["Sunsilk shampoo Big"],
    "Vaseline Coco Radiant": ["Original Vaseline Coco Radiant 200ml"],
    "Vaseline Dry Skin Repair": ["Original Vaseline Dry Skin Repair 200ml"],
    "Vaseline PJ Original": ["Vaseline Pj Original 45ml"],
    "Nunu Vaseline": ["Nunu Vaseline"],
    "Florida Glycerin": ["Florida Glycerin (50cc)", "Florida Glycerin (70cc)"],
    "Sunny Body Jel": ["Sunny Body Jel", "Sunny Body Jel"],
    "Sunny Shampoo": ["Sunny Shampoo", "Sunny Shampoo"],

    # --- SOAPS & CLEANING ---
    "555 Laundry Soap": ["555 Laundry soap"],
    "555 Liquid Detergent": ["555 Liquid detergent"],
    "Bleach": ["Bleach"],
    "Ghion Bleach": ["Ghion bleach"],
    "Home 220g Laundry Soap": ["Home 220g Laundry Soap", "Home 220g Laundry Soap"],
    "Solar Laundry Soap": ["Solar Laundry soap"],
    "Crown Laundry Soap (250g)": ["Crown Laundary Soap (250g)"],
    "Crown Laundry Soap (150g)": ["Crown Laundry Soap (150g)"],
    "Crown Powder Detergent (180gm)": ["Crown Powder Detergent (180gm)"],
    "Rose Laundry Soap (250g)": ["Rose Laundry Soap (250g)"],
    "Largo Liquid Detergent": ["Largo Liquid Detergent", "Largo Liquid Detergent"],
    "Liquid Cloth Soap": ["Liquid Cloth Soap"],
    "Lifebuoy Red (70g)": ["Lifebuoy Red (70g)"],
    "Lux Soft Touch Soap Bar": ["Lux soft touch soap bar"],
    "Ipas Anti-Bacterial Soap": ["Ipas Anti-Bacterial Soap", "Ipas antibacterial soap"],
    "Kono Beauty Soap": ["Kono beauty soap"],
    "Diva Bar Soap": ["Diva Bar Soap", "Diva Bar Soap"],
    "BYMT Dish Soap": ["BYMT Dish Soap"],
    "Tumha Dish Wash": ["Tumha Dish Wash", "Tumha Dish Wash"],
    "Tumha Hand Wash": ["Tumha Hand Wash"],
    "Tumha Laundry Detergent": ["Tumha Laundry Detergent", "Tumha Laundry Detergent"],
    "Yon-X Laundry Detergent": ["Yon-X Laundry Detergent"],
    "Yon-X Liquid Dish Wash": ["Yon-X Liquid Dish Wash"],
    "Cloud Dish Wash": ["Cloud Dish Wash"],
    "Cloud Dish Wash / 5L": ["Cloud Dish Wash / 5L"],
    "Cloud Dish Wash / 750ML": ["Cloud Dish Wash / 750ML"],
    "Cloud Toilet Cleaner": ["Cloud Toilet Cleaner"],
    "Sunny Dish Wash": ["Sunny Dish Wash", "Sunny Dish Wash"],
    "Sunny Hand Wash": ["Sunny Hand Wash", "Sunny Hand Wash"],
    "Sunny Detergent / 5L": ["Sunny Detergent / 5L"],
    "Sunny Detergent / 800ML": ["Sunny Detergent / 800ML"],
    "Sunny Multi Purpose": ["Sunny Multi Purpose"],
    "Sunny Oxidizer": ["Sunny Oxidizer", "Sunny Oxidizer"],
    "Sunny Softener": ["Sunny Softener", "Sunny Softener"],
    "Sunny Window Cleaner": ["Sunny Window Cleaner", "Sunny Window Cleaner"],
    "YALI Dish Wash 5lit": ["YALI Dish Wash 5lit"],
    "YALI Dish Wash 750ml": ["YALI Dish Wash 750ml"],
    "YALI Hand Wash 500ml": ["YALI Hand Wash 500ml"],
    "YALI Hand Wash 5lit": ["YALI Hand Wash 5lit"],
    "YALI Laundary Detergent 5lit": ["YALI Laundary Detergent 5lit"],
    "Yali Laundry Detergent 5lit": ["Yali Laundry detergent 5lit"],
    "YALI Powder Cleaner": ["YALI Powder cleaner"],
    "YALI Window Cleaner 750ml": ["YALI Window Cleaner 750ml"],
    "Safe Soft": ["Safe Soft"],
    "Multi Purpose": ["Multi Purpose"],
    "Multi Purpose 1L": ["Multi purpose 1L"],
    "Multi Purpose 2L": ["Multi purpose 2L"],

    # --- FOODS, SPICES & INGREDIENTS ---
    "2Bf Chocolate": ["2Bf Chocolate"],
    "Choco Balls": ["Choco Balls"],
    "Coco Crunch": ["Coco Crunch"],
    "Fruity Rings": ["Fruity Rings"],
    "Finger Biscuits": ["Finger Biscuits"],
    "Juliet Biscuit": ["Juliet Biscuit"],
    "Kalos Cookies": ["Kalos Cookies"],
    "Cookies": ["Cookies"],
    "Moon Coffee Biscuit": ["Moon Coffee Biscuit"],
    "Moon Cinnamon Biscuit": ["Moon Cinnamon Biscuit"],
    "Moon Strawberry Biscuit": ["Moon Strawberry Biscuit"],
    "Moya Biscuit The Saint": ["Moya Biscuit The Saint"],
    "Moya Coco Loops Biscuit": ["Moya coco loops Biscuit"],
    "NIB Chocolate Bar": ["NIB Chocolate Bar"],
    "NIB Chocolate Spread": ["NIB Chocolate Spread", "Nib Chocolate Spread"],
    "NIB Dark Mini Chocolate (10 pieces)": ["NIB Dark Mini Chocolate (10 pieces)"],
    "NIB Mini Chocolate": ["NIB Mini Chocolate", "Nub Mini Chocolate (5pcs)"],
    "Aymi Geda Flour": ["Aymi Geda Flour"],
    "Biya Weya Flour": ["Biya Weya Flour", "Biya Weya Flour", "Biya Weya Flour"],
    "Kojj Flour": ["Kojj Flour"],
    "Maleda Flour": ["Maleda Flour", "Maleda Flour"],
    "Mawel Flour": ["Mawel Flour"],
    "Rahmet Flour": ["Rahmet Flour"],
    "Flour (3 Kilogram)": ["Flour (3 Kilogram)"],
    "Arkee Basmati Rice": ["Arkee Basmati Rice"],
    "City Bird Basmati Rice": ["City Bird Basmati Rice"],
    "Indian Cheers Basmati Rice": ["Indian Cheers Basmati Rice"],
    "Indian Golden Basmati Rice": ["Indian Golden Basmati Rice"],
    "Red Rose Basmati Rice": ["Red Rose Basmati Rice"],
    "Semira Basmti Rice": ["Semira Basmti Rice"],
    "Loose Granola (Cereal)": ["Loose granola (Cereal)"],
    "Taza Granola": ["Taza Granola"],
    "Taza Quick Oats": ["Taza Quick oats"],
    "Quaker White Oats": ["Quaker White Oats"],
    "Teff Flakes": ["Teff Flakes"],
    "Almi Berbere": ["Almi Berbere"],
    "Almi Mitin Shiro": ["Almi Mitin Shiro"],
    "Befrekot Mitin Shiro": ["Befrekot Mitin Shiro"],
    "Befrekot Pepper": ["Befrekot Pepper"],
    "Habsha Pepper": ["Habsha Pepper"],
    "Hana Mitmita": ["Hana Mitmita"],
    "Kitfo Spice": ["Kitfo Spice"],
    "Mixed Spice": ["Mixed Spice"],
    "Tea Spice": ["Tea Spice"],
    "Grinded Rosemary": ["Grinded Rosemary"],
    "Rosemary": ["Rosemary", "Rosemary"],
    "Cardamon": ["Cardamon"],
    "Cinnamon Powder": ["Cinnamon Powder"],
    "Turmeric": ["Turmeric"],
    "Vanilla Flavoring Essence": ["Vanilla flavoring essence"],
    "Baking Powder": ["Baking Powder"],
    "Chapa Baking Powder": ["Chapa Baking Powder"],
    "Brown Sugar (Fine)": ["Brown Sugar (Fine)"],
    "Pure Iodized Salt": ["Pure Iodized Salt"],
    "Consul Olive Oil": ["Consul Olive Oil", "Cousul Olive Oil"],
    "Momin Sunflower Oil": ["Momin Sunflower Oil"],
    "Nura Sunflower Oil": ["Nura Sunflower Oil"],
    "Okapi Sunflower Oil": ["Okapi Sunflower Oil"],
    "Orkide Sunflower Oil": ["Orkide sunflower oil"],
    "Sunvito Sunflower Oil": ["Sunvito Sunflower Oil"],
    "Zehabu Sunflower Oil": ["Zehabu Sunflower Oil"],
    "Mayra Sunflower Oil": ["Mayra sunflower oil"],
    "Omaar Light Meat Tuna (Large)": ["Omaar light meat tuna(large)"],
    "Almadina Saad Dates": ["Almadina Saad Dates", "Saad Dates"],
    "Applack Baby Formula 1": ["Applack Baby Formula 1"],
    "Armella Mixed Fruit Jam": ["Armella Mixed Fruit Jam"],
    "Dachi Strawberry Jam (450g)": ["Dachi Strawberry Jam (450g)"],
    "Fargello Mango Juice": ["Fargello Mango Juice"],
    "Hamda Powdered Milk": ["Hamda Powdered Milk"],
    "Nido 400gram Milk Powder": ["Nido 400gram Milk Powder"],
    "Zagol Cheese": ["Zagol Cheese"],
    "Zagol Milk": ["Zagol Milk", "Zagol Milk"],
    "Zagol Table Butter": ["Zagol Table Butter"],
    "Oche Fasting Butter": ["Oche Fasting Butter"],
    "Butter Spice": ["Butter Spice"],
    "Fish Fillet": ["Fish Fillet"],
    "Foreign’s Egg": ["Foreign’s egg"],
    "Groceries": ["Groceries"],
    "Gursha Bars": ["Gursha bars"],
    "Individual Bars": ["Individual bars"],
    "Kuri Lactation Tea": ["Kuri Lactation Tea"],
    "Mama's Choice": ["Mama's choice"],
    "Mama's Choice with Fruits": ["Mama's choice with fruits"],
    "Sosi Soya": ["Sosi Soya"],
    "Tasty Soya": ["Tasty Soya"],
    "Torti Chips": ["Torti Chips", "Torti Chips (30g)"],
    "Raw Pop Corn": ["Raw Pop Corn", "Raw Popcorn 500g"],
    "Wild Coffee": ["Wild Coffee"],
    "Jimma Coffee": ["Jimma Coffee"],
    "Fenet Grinded Coffee": ["Fenet Grinded Coffee"],
    "Coffee (Wollega)": ["Coffee(Wollega)"],
    "Chito Coffee": ["Chito Coffee", "Chito Coffee"],

    # --- PACKAGES & BUNDLES ---
    "All in One Package": ["All in One Package"],
    "Asibeza Tikil Bundle": ["Asibeza tikil bundle"],
    "Delicious Package": ["Delicious Package"],
    "Easter Package": ["Easter Package"],
    "Fruit Package Bundle": ["Fruit package  bundle"],
    "Hassle Free Package": ["Hassle Free Package"],
    "Holiday Package": ["Holiday Package"],
    "Home Ready Package": ["Home Ready Package"],
    "Kitfo Holiday Package": ["Kitfo Holiday Package"],
    "Kitfo Package": ["Kitfo Package"],
    "Mix Package": ["Mix Package"],
    "Mix Package Bundle": ["Mix package bundle"],
    "Special Mix Package": ["Special Mix package"],
    "Special Mutton Package": ["Special Mutton Package"],
    "Vegetable Package Bundle": ["Vegetable package  bundle"],
    "Yummy Package": ["Yummy Package"],
    "Kushnaye Tikil": ["ኩሽናዬ ጥቅል"],
    "Gebeya Lemne Tikil": ["ገበያ ለምኔ ጥቅል"],

    # --- ELECTRONICS & MISC ---
    "T-500 Smartwatch": ["T-500 Smartwatch"],
    "ChipChip Umbrella": ["ChipChip Umbrella"],
    "Elf Primer": ["Elf Primer"],
    "Five Star Safety Matches": ["Five Star Safety Matches"],
    "Ipen": ["Ipen"],
    "Azzy Multi Functional": ["Azzy multi functional"],
    "Jam Comedy Night": ["Jam Comedy Night"],

    # --- REMAINING MISSING ITEMS ---
    "My Kishin 3 Pieces": ["My Kishin / 3 Pieces"],
    "My Kishin 5 Pieces": ["My kishin / 5 Pieces"],
    "Ox Kircha For 10": ["Ox Kircha - for 10"],
    "Ox Kircha For 6": ["Ox Kircha - for 6"],
    "Pea": ["Pea"],
    "Power Tissue": ["Power Tissue"],
    "Prima Macaroni": ["Prima Macaroni"],
    "Prima Pasta": ["Prima Pasta"],
    "Safa Ketchup": ["Safa Ketchup"],
    "Signal Toothpaste (Medium)": ["Signal Toothpaste (Medium)"],
    "Twins Facial Tissue": ["Twins Facial Tissue"],
    "Twins Paper Towel": ["Twins Paper Towel"],
    "Twins Table Napkin": ["Twins Table Napkin"],
    "Twins Toilet Tissue": ["Twins Toilet Tissue"],
    "Viva Toilet Paper Tissue": ["Viva Toilet Paper Tissue"],
    "Knorr 5 Piece": ["Knorr /5piece"],
    "Kojj Pastina": ["Kojj Pastina"],
    "Elbow Macaroni": ["Elbow Macaroni", "Elbow Macaroni (Small)"],
    "Indomie Noodles (Large)": ["Indomie Noodles (Large)"],
    "Ok Pasta": ["Ok Pasta"],
    "Ok Vegetable Noodle": ["Ok Vegetable Noodle"],
    "Loli Chips Ketchup Flavor": ["Loli Chips Ketchup Flavor"],
    "Loli Chips Paprika": ["Loli chips (Paprika)"],
    "Snail Hair Oil": ["Snail hair oil"],
    "Zenith Hair Oil": ["Zenith hair oil"],
    "Elsa Kolo": ["Elsa Kolo"],
    "System Metadata": ["Product name", "Item name"]

}


# ==============================================================================
#  DYNAMIC MAPPING LOADING
# ==============================================================================

def load_mapping_rules_from_db():
    """
    Attempts to load mapping rules from the `product_mapping_rules` table.
    Returns a tuple (parent_child_mapping, child_to_parent_map).
    If loading fails, returns (None, None).
    """
    try:
        engine = get_db_engine('hub')
        
        # Check if table exists first to avoid error spam if migration hasn't run
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = 'hub_mapping_rules'
                )
            """))
            if not result.scalar():
                print("⚠️  Table 'hub_mapping_rules' not found. Using static dictionary.")
                return None, None

            # Fetch active rules. Use LEFT JOIN so rules don't disappear if master table is in flux
            query = text("""
                SELECT r.input_pattern, COALESCE(p.parent_product_name, r.input_pattern) as parent_name
                FROM hub_mapping_rules r
                LEFT JOIN hub_standard_products p ON r.parent_product_id = p.parent_product_id
                WHERE r.is_active = TRUE
                ORDER BY r.priority DESC
            """)
            rules = conn.execute(query).fetchall()
            
            if not rules:
                print("⚠️  No active rules found in database. Using static dictionary.")
                return None, None

            # Build dictionaries
            db_parent_child = {}
            db_child_parent = {}
            
            for pattern, parent_name in rules:
                clean_pattern = re.sub(r'\s+', ' ', str(pattern)).strip().lower()
                
                # Update Child -> Parent Map (Last one wins if duplicates exists, but we ordered by priority)
                if clean_pattern not in db_child_parent:
                     db_child_parent[clean_pattern] = parent_name
                
                # Update Parent -> Child List
                if parent_name not in db_parent_child:
                    db_parent_child[parent_name] = []
                db_parent_child[parent_name].append(pattern)

            print(f"✅ Loaded {len(db_child_parent)} mapping rules from database.")
            return db_parent_child, db_child_parent

    except Exception as e:
        print(f"❌ Failed to load mappings from DB: {e}. Using static dictionary.")
        return None, None

# 1. Try to load from DB
DB_PARENT_CHILD_MAPPING, DB_CHILD_TO_PARENT_MAP = load_mapping_rules_from_db()

# 2. Rename the static dictionary to DEFAULT for clarity/fallback
DEFAULT_PARENT_CHILD_MAPPING = PARENT_CHILD_MAPPING

def _create_child_to_parent_map(mapping):
    """Creates a reverse mapping from a cleaned child name to its parent name."""
    child_map = {}
    for parent, children in mapping.items():
        for child in children:
            cleaned_child = re.sub(r'\s+', ' ', child).strip().lower()
            child_map[cleaned_child] = parent
            
    return child_map

# 3. Determine Final Mappings
if DB_PARENT_CHILD_MAPPING and DB_CHILD_TO_PARENT_MAP and not os.environ.get('FORCE_STATIC_MAPPING'):
    # Use DB mappings
    PARENT_CHILD_MAPPING = DB_PARENT_CHILD_MAPPING
    CHILD_TO_PARENT_MAP = DB_CHILD_TO_PARENT_MAP
else:
    # Fallback to Static
    CHILD_TO_PARENT_MAP = _create_child_to_parent_map(DEFAULT_PARENT_CHILD_MAPPING)


def _generate_stable_uuid(name):
    """Generates a consistent UUID5 based on a given name."""
    return str(uuid.uuid5(NAMESPACE_UUID, name))


def create_parent_child_master_table(all_product_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the hub_standard_products table with OPTION 3 structure:
    - parent_product_id: UUID for the parent
    - parent_name: canonical parent product name
    - created_at: earliest timestamp for this parent
    
    ONE ROW PER PARENT (minimal, clean structure)
    Child information is stored in source tables via parent_product_id foreign key.
    """
    print("\n--- Generating hub_standard_products Table (Option 3) ---")

    if all_product_data_df.empty:
        print(" -> Input data is empty. No master table to generate.")
        return pd.DataFrame()

    df = all_product_data_df.copy().dropna(subset=['raw_product_name'])
    
    # Filter out invalid entries
    initial_count = len(df)
    df = df[df['raw_product_name'].astype(str).str.strip() != '0']
    
    # Filter out garbage/placeholder data entries
    garbage_values = ['item name', 'product name', 'test', 'n/a', 'na', 'none', '']
    df = df[~df['raw_product_name'].astype(str).str.strip().str.lower().isin(garbage_values)]
    
    final_count = len(df)
    if initial_count > final_count:
        print(f" -> Filtered out {initial_count - final_count} invalid/garbage records.")

    print("Step 1: Mapping products to parents using PARENT_CHILD_MAPPING...")
    df['cleaned_name'] = df['raw_product_name'].apply(lambda x: re.sub(r'\s+', ' ', str(x)).strip().lower())
    df['parent_name'] = df['cleaned_name'].map(CHILD_TO_PARENT_MAP)
    
    # For unmapped products, use the original name as parent
    df['parent_name'] = df['parent_name'].fillna(df['raw_product_name'])

    print("Step 2: Converting created_at to consistent format...")
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
    
    print("Step 3: Aggregating by parent to get earliest created_at and source...")
    # Group by parent and get the earliest created_at and first source
    parent_groups = df.groupby('parent_name').agg({
        'created_at': lambda x: x.dropna().min() if not x.dropna().empty else None,
        'source_db': lambda x: x.iloc[0] if len(x) > 0 else None
    }).reset_index()
    
    print("Step 4: Generating parent_product_id...")
    parent_groups['parent_product_id'] = parent_groups['parent_name'].apply(_generate_stable_uuid)
    
    print("Step 5: Finalizing canonical master table...")
    # Select final columns in the correct order
    canonical_df = parent_groups[[
        'parent_product_id',
        'parent_name',
        'source_db',
        'created_at'
    ]].copy()
    
    # Rename source_db to source
    canonical_df = canonical_df.rename(columns={'source_db': 'source'})
    
    # Sort by parent_name
    canonical_df = canonical_df.sort_values('parent_name').reset_index(drop=True)
    
    print(f" -> Created hub_standard_products with {len(canonical_df)} parent products.")
    print(f" -> Child products are linked via parent_product_id in source tables.")
    return canonical_df

# ==============================================================================
#  2. SUPABASE FETCH & EXECUTION LOGIC
# ==============================================================================

# Add parent directory to path to reach utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def main():
    try:
        supabase_engine = get_db_engine('supabase')
        hub_engine = get_db_engine('hub')
        
        print("\n🚀 CONNECTING TO REMOTE SOURCES...")

        # 1. Fetch Supabase Data (Remote - ONLY products)
        print("🌐 Fetching from Remote Supabase (products)...")
        with supabase_engine.connect() as conn:
            df_sup_p = pd.read_sql("SELECT id as raw_product_id, name as raw_product_name, created_at FROM products", conn)
        print(f"✅ Supabase: Fetched {len(df_sup_p)} products.")

        # 2. Fetch Staging Data (Replaces ClickHouse/Superset)
        from pipeline.data_loader import load_product_data_from_staging
        
        # We pass hub_engine just to satisfy signature if needed, or create a staging engine
        # But load_product_data_from_staging requires an engine.
        # We need to create a staging engine here.
        staging_engine = get_db_engine('staging')
        df_staging = load_product_data_from_staging(staging_engine)
        
        # 3. Combine ALL Data
        # df_staging already contains products and product_names
        all_product_data = pd.concat([df_sup_p, df_staging], ignore_index=True)
        
        # 4. Generate the Canonical Master Table
        master_df = create_parent_child_master_table(all_product_data)

        if master_df.empty:
            print("❌ Standardization failed. No data to save.")
            return

        # 5. Create a Lookup for Parent IDs (raw_name -> parent_id)
        # We use a case-insensitive, whitespace-trimmed approach for safety
        print("\n📝 Creating mapping lookup...")
        
        # Helper to get parent_id for many names
        def get_parent_id(name):
            if not name: return None
            cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
            parent_name = CHILD_TO_PARENT_MAP.get(cleaned)
            if not parent_name:
                # Fallback to identify unmapped canonical name (same logic as standardization)
                parent_name = name # For unmapped, the name is its own parent
            
            # Stable UUID from the parent name
            return str(_generate_stable_uuid(parent_name))

        # Helper to get parent name and status for the verification table
        def get_parent_status(name):
            if not name: return pd.Series([None, "❌"])
            cleaned = re.sub(r'\s+', ' ', str(name)).strip().lower()
            parent_name = CHILD_TO_PARENT_MAP.get(cleaned)
            if parent_name:
                return pd.Series([parent_name, "✅"])
            else:
                return pd.Series([name, "❌"]) # If not mapped, it's its own parent, status is '❌'

        # 6. Update Local PostgreSQL Tables in pgAdmin
        print("\n💾 SAVING RESULTS TO LOCAL POSTGRES (HUB)...")
        
        with hub_engine.begin() as conn:
            # A. Update hub_supabase_results local copy
            print(" -> [1/3] Updating local 'hub_supabase_results'...")
            local_sup_p = pd.read_sql("SELECT * FROM supabase_products", conn)
            # Only add the ID column
            local_sup_p['parent_product_id'] = local_sup_p['name'].apply(get_parent_id)
            local_sup_p.to_sql('hub_supabase_results', conn, if_exists='replace', index=False, method='multi', chunksize=500)

            # B. Update hub_staging_results local copy
            print(" -> [2/3] Updating local 'hub_staging_results'...")
            local_ch_p = pd.read_sql("SELECT * FROM clickhouse_product_names", conn)
            # Only add the ID column
            local_ch_p['parent_product_id'] = local_ch_p['name'].apply(get_parent_id)
            local_ch_p.to_sql('hub_staging_results', conn, if_exists='replace', index=False, method='multi', chunksize=500)

            # C. Save the final Canonical Master Table
            print(" -> [3/3] Creating 'hub_standard_products' table...")
            master_df.to_sql('hub_standard_products', conn, if_exists='replace', index=False, method='multi', chunksize=500)

        print("\n" + "="*80)
        print("✅ SUCCESS: ALL TABLES UPDATED IN LOCAL HUB!")
        print("   - Table: hub_supabase_results (parent_product_id)")
        print("   - Table: hub_staging_results (parent_product_id)")
        print("   - Table: hub_standard_products (Master parent list)")
        print("="*80)

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
