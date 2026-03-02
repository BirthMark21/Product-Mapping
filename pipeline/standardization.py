#!/usr/bin/env python3
import os
import sys
import uuid
import re
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from utils.db_connector import get_db_engine

load_dotenv()

NAMESPACE_UUID = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

PARENT_CHILD_MAPPING = {
    "12 Piece Chicken": ["Chicken", "Chicken Groceries", "Regular Chicken Package", "Special Chicken Package", "BGS Foreign Chicken", "12 Piece Chicken", "12 piece Chicken", "Chicken Package", "Habesha Chicken", "Habesha Chicken Package"],
    "2Bf Chocolate": ["2Bf Chocolate"],
    "555 Laundry Soap": ["555 Laundry Soap", "555 Laundry soap"],
    "555 Liquid Detergent": ["555 Liquid Detergent", "555 Liquid detergent"],
    "ABC Diaper": ["ABC Diaper", "ABC Daiper"],
    "Addis Tea": ["Addis tea", "Addis Tea"],
    "Addis Tea Bag": ["Addis Tea Bag"],
    "Afar Salt": ["Afar Salt"],
    "Afro EIIDE Exercise Book": ["Afro Eiide Exercise book", "Afro EIIDE Exercise Book", "EIIDE Exercise Book (12 pieces)"],
    "Aja Kinche": ["Aja kinche", "Aja Kinche"],
    "Ajax Soap": ["Ajax Soap", "ajax soap", "Ajax (Large)"],
    "Akkoo Coffee": ["Akkoo Coffee"],
    "AL-Hinan Flour": ["AL-Hinan Flour", "AL-Hinan flour", "AL-Hinan Food Complex", "al Hilal flour"],
    "Alem Card Holder": ["Alem Card Holder"],
    "All in One Package": ["All in One Package"],
    "Almadina Saad Dates": ["Almadina Saad Dates", "Saad Dates"],
    "Almi Berbere": ["Almi Berbere"],
    "Almi Dabo Kolo": ["Almi dabo kolo", "Almi Dabo Kolo", "Almi Dabo Kolo "],
    "Almi Kolo": ["Almi kolo", "Almi Kolo", "Almi Kolo"],
    "Almi Mitin Shiro": ["Almi Mitin Shiro"],
    "Aloha Conditioner": ["Aloha Conditioner", "Aloha conditioner"],
    "Aluu Sunflower Oil": ["Aluu Pure Sunflower Oil", "Aluu Sunflower Oil"],
    "AMG Coffee": ["AMG Coffee", "AMG coffee", "Amg Coffee 50g", "AMG Coffee (Grinded)", "AMG Coffee (Roasted)", "AMG Grinded Coffee"],
    "Ami White Honey": ["Ami White Honey", "ami white honey", "AMI White Honey"],
    "Applack Baby Formula 1": ["Applack Baby Formula 1"],
    "Apple": ["Apple", "Appel", "Apple B"],
    "Basmati Rice": ["Basmati Rice", "basmati rice 5kg"],
    "Kismet Basmati Rice": ["Kismet Basmati Rice"],
    "Armella Mixed Fruit Jam": ["Armella Mixed Fruit Jam"],
    "Asibeza Tikil Bundle": ["Asibeza Tikil Bundle", "Asibeza tikil bundle"],
    "Ater Kik": ["Ater Kik"],
    "Avocado": ["Avocado", "Avocado A", "Avocado OG", "Avocado Shekaraw", "Local Avocado"],
    "Avocado B": ["Avocado B"],
    "Avocado Hair Oil": ["Avocado Hair Oil", "Avo avocado hair oil"],
    "Avocado Raw": ["Avocado Raw"],
    "Avocado Ripe": ["Ripe Avocado", "Avocado Ripe"],
    "Avo Carrot Hair Oil": ["Avo Carrot Hair Oil", "Avo carrot", "avo avocado hair oil", "Avo carrot hair oil"],
    "Aymi Geda Flour": ["Aymi Geda Flour"],
    "Azzy Multi Functional": ["Azzy Multi Functional", "Azzy multi functional"],
    "Baby Diaper": ["Baby Diaper", "B&B Baby diaper no1 to 5", "B&B Baby Diaper"],
    "Baby Wipes": ["Baby Wipes", "B&b baby wipes", "B&B Baby Wipes", "ABC baby wipes", "Good Baby Wipes"],
    "Bahja Sunflower Oil": ["Bahja Sunflower Oil"],
    "Baking Powder": ["Baking Powder"],
    "Banana": ["Banana", "Banana/ Raw ", "Banana/ Raw"],
    "Barley Kinche": ["Barley Kinche", "Yegebes Kinche", "kinche yegebis", "Kinche Yegebs"],
    "Beetroot": ["Beetroot", "beetroot", "Beet root"],
    "Beetroot Small Size": ["Beetroot Small Size"],
    "Befrekot Mitin Shiro": ["Befrekot Mitin Shiro"],
    "Befrekot Pepper": ["Befrekot Pepper"],
    "Bella Sanitary Pad": ["Bella sanitary pad", "Bella Sanitary Pad", "Bella Sanitary Pad ", "Eve sanitary pad"],
    "Belt Bag": ["Belt Bag"],
    "Besobila": ["Besobila"],
    "Bethel Flour": ["bethel flour", "Bethel flour ", "Bethel flour", "Bethel Flour"],
    "Big Bar Packs": ["Big Bar Packs", "Big bar packs"],
    "Big Bites Pack": ["Big Bites Pack", "Big bites pack"],
    "Bizce Sunflower Oil": ["Bizce Sunflower Oil"],
    "Biya Weya Flour": ["Biya Weya Flour"],
    "Black Lion Tea": ["Black Lion Tea", "Black lion tea", "Black Lion (40g)", "Black Lion Tea (80g)"],
    "Bleach": ["Bleach", "bleach 5l"],
    "Bonga Honey": ["Bonga Honey", "Bonga Mar"],
    "Bourbon Cream Biscuit": ["Bourbon Cream Biscuit", "Bourbon cream biscuit", "bourbon cream biscuit", "Bourbon Cream Biscut"],
    "Bravo Toilet Paper": ["Bravo Toilet Paper", "bravo toilet paper"],
    "bravo table tissue": ["bravo table tissue", "Bravo Table Soft", "Bravo Table Tissue"],
    "Bright Detergent": ["Bright Detergent", "bright detergent"],
    "Broccoli": ["Broccoli", "Broccolis"],
    "Brown Sugar (Fine)": ["Brown Sugar (Fine)"],
    "Buna Pen": ["buna pen", "Buna Pen"],
    "Buna Pencil": ["Buna Pencil", "Buna pencil"],
    "Butter Cups Biscuit": ["Butter Cups Biscuit", "Butter cups biscuit", "Butter Cups Fasting Biscuit", "Beka ButterCups Fasting Biscut"],
    "Butter Spice": ["Butter Spice"],
    "BYMT Dish Soap": ["BYMT Dish Soap"],
    "Canvas Bag": ["Canvas Bag"],
    "Cardamon": ["Cardamon"],
    "Cardigan": ["Cardigan", " Cardigan"],
    "Carrot": ["Carrot", "Carrots"],
    "Carrot B": ["Carrot B"],
    "Casual Sweater": ["Casual Sweater"],
    "Casual T-Shirt": ["Casual T-Shirt"],
    "Cauliflower": ["Cauliflower"],
    "Celery": ["Celery", "Parsley", "Leek", "Coriander"],
    "Cerifam Fruit": ["Cerifam fruit", "cerifam fruit", "Cerifam Fruit ", "Cerifam Fruit"],
    "Cerifam Normal": ["Cerifam Normal"],
    "Cerifam Vegetable": ["Cerifam vegetable", "cerifam vegetable", "Cerifam Vegetable ", "Cerifam Vegetable"],
    "Chapa Baking Powder": ["Chapa Baking Powder"],
    "Cheese 500g": ["Cheese", "Ethiopian Cheese", "Cheese 500g", "አይብ"],
    "Cheese Package": ["Cheese Package", "አይብ ጥቅል"],
    "Chickpea": ["Chickpea", "Shembera"],
    "Chili Green": ["Chili Green", "Chilly Green", "Chilly green"],
    "Chilly Green (Starta)": ["Chilly Green (Starta)", "Green Chili", "Green Chilli", "Green Chili (ስታርታ)", "Chilly Green (Starter)"],
    "Chilly Green Elfora": ["Chilly Green Elfora", "Chilly Green (Elfora)"],
    "Chilly Short": ["Chilly Short", "Chilly short", "Chilli short", "Chili short"],
    "ChipChip Umbrella": ["ChipChip Umbrella"],
    "Chito Coffee": ["Chito Coffee", "Chito Coffee"],
    "Choco Balls": ["Choco Balls"],
    "Cinnamon Powder": ["Cinnamon Powder"],
    "Cinni Krunches Biscuit": ["Cinni Krunches Biscuit", "Cinni Krunches biscuit", "Cinni Krunches Fasting Biscuit", "Beka Cinni Fasting Biscuit"],
    "City Bird Basmati Rice": ["City Bird Basmati Rice"],
    "CK Powdered Soap": ["CK laundary soap", "CK Powdered Soap", "Ck Powder Soap", "Ck powdered soap", "CK Laundry Soap", "CK Powdered Soap (40g)", "Ck powderd soap 40g", "ck powderd soap 150g", "ck powderd soap 1kg", "ck powderd soap 40g", "ck powderd soap 500g", "ck powderd soap 5kg"],
    "Cleaning Bundle": ["Cleaning Bundle"],
    "Cloud Bleach": ["cloud bleach 5L", "Cloud bleach ", "Cloud bleach ", "Cloud bleach", "Cloud Bleach", "Cloud Bleach / 1L", "Cloud Bleach / 5L"],
    "Cloud Dish Wash": ["Cloud Dish Wash", "Cloud Dish Wash / 5L", "Cloud Dish Wash / 750ML"],
    "Cloud Hand Wash": ["Cloud hand wash 5l", "Cloud Hand Wash"],
    "Cloud Multipurpose": ["Cloud Multipurpose", "Cloud multipurpose", "cloud multipurpose", "Cloud Multi Purpose", "Cloud Multi Purpose/ 1L", "Cloud Multi Purpose/ 2L", "Cloud Multi Purpose/ 5L", "Cloud multi purpose 5l"],
    "Cloud Toilet Cleaner": ["Cloud Toilet Cleaner"],
    "Cloud Window Cleaner": ["Cloud Window Cleaner", "cloud window cleaner"],
    "Coco Crunch": ["Coco Crunch"],
    "Coffee (Wollega)": ["Coffee (Wollega)", "Coffee(Wollega)"],
    "Coffee Late Cream Biscuit": ["Coffee Late Cream Biscuit", "Coffee late cream biscuit", "coffee late cream biscuit", "Coffee Latte Cream Biscuit"],
    "Consul Olive Oil": ["Consul Olive Oil", "Cousul Olive Oil"],
    "Cookies": ["Cookies"],
    "Corn": ["Corn"],
    "Corn Kinche": ["Corn kinche", "Corn Kinche ", "Corn Kinche"],
    "Corn Starch": ["Corn starch", "Corn Starch ", "Corn Starch"],
    "Cotton T-Shirt": ["Cotton T-Shirt", "Cotton T-shirt"],
    "Cross-body Bag": ["Cross-body Bag", "Crossbody Bag"],
    "Crown Laundry Detergent": ["Crown Laundry Detergent", "Crown powdered detergent (30g)", "Crown Powder Detergent (30g)", "Crown Powder Detergent (180gm)"],
    "Crown Laundry Soap": ["Crown Laundry Soap (150g)", "Crown Laundary Soap (250g)", "Crown powdered soap (180g)", "Crown Laundry Soap"],
    "Cucumber": ["Cucumber", "Cucumbers"],
    "Custard Pie": ["Custard Pie"],
    "Dabur Herbal Toothpaste": ["Dabur Herbal Toothpaste", "Dabur herbal tooth paste 150g", "Dabur herbal tooth paste 30g", "Dabur herbal tooth paste 50g", "Dabur Herbal Tooth Paste", "Dabur Tooth Paste (50g)"],
    "Dachi Ketchup": ["Dachi ketchiup", "dachi ketchiup", "Dachi Ketchup", "Dachi Ketchup ", "Dachi Kethup"],
    "Dachi Strawberry Jam (450g)": ["Dachi Strawberry Jam (450g)"],
    "Dachi Vimto": ["dachi vimto", "Dachi Vimto"],
    "Dachi Vinegar": ["dachi vinegar", "Dachi Vinegar"],
    "Dainty Natural Shea Butter": ["Dainty Natural Shea Butter", "Dainty natural shea butter"],
    "Dania Oil": ["Dania Oil", "Dania oils", "Dania Sunflower Cooking Oil"],
    "Dania rice": ["Dania rice", "Dania Basmati Rice"],
    "Dawedo Pepper": ["Dawed pepper ", "Dawed pepper", "Dawedo Pepper ", "Dawedo Pepper"],
    "Delicious Package": ["Delicious Package"],
    "Dexe Black Hair Shampoo": ["Dexe Black Hair Shampoo", "Dexe Black Hair shampoo"],
    "Dh geda flour": ["DK GEDA FLOUR", "Dh geda flour"],
    "Diana Soap": ["Diana Soap", "Diana soap 80g", "Diana Orange Toilet Soap (80gm)", "Diana Toilet Soap (20gm)", "Diana Toilet Soap (25gm)", "Diana Soap (80gm)"],
    "Difo Package": ["Difo Package", "Difo package ", "Difo package", "ድፎ ጥቅል", "ዳቦ ጥቅል", "Defo Package", "Special Defo package"],
    "Digis Table Salt": ["Digis Table Salt", "Digis table salt", "digis table salt"],
    "Dish Wash": ["Dish Wash"],
    "Diva Bar Soap": ["Diva Bar Soap", "Diva Bar Soap"],
    "Duru Soap": ["DURU soap", "Duru Soap", "Duru Soap (180g)"],
    "Easter Package": ["Easter Package"],
    "Egg": ["Egg"],
    "Eggplant": ["Eggplant", "Egg plant", "eggplant"],
    "Elbow Macaroni": ["Elbow Macaroni", "Elbow Macaroni (Small)"],
    "Elf Primer": ["Elf Primer"],
    "Essential Pack": ["Essential Pack"],
    "Elsa Kolo": ["Elsa Kolo"],
    "Fafa Baby Food": ["Fafa Baby Food", "Fafa baby food"],
    "Fargello Mango Juice": ["Fargello Mango Juice"],
    "Fenet Grinded Coffee": ["Fenet Grinded Coffee"],
    "Finger Biscuits": ["Finger Biscuits"],
    "Fish Fillet": ["Fish Fillet"],
    "Five Star Safety Matches": ["Five Star Safety Matches"],
    "Flax Seed": ["Flax seed", "Flax Seed ", "Flax Seed"],
    "Florida Glycerin": ["Florida Glycerin", "Florida Glycerin (50cc)", "Florida Glycerin (70cc)"],
    "Flour": ["Flour", "flour"],
    "Flour (3 Kilogram)": ["Flour (3 Kilogram)"],
    "Football Jersey": ["Football Jersey"],
    "Foreign’s Egg": ["Foreign’s Egg", "Foreign’s egg"],
    "Fruit Package Bundle": ["Fruit Package Bundle", "Fruit package  bundle"],
    "Fruity Rings": ["Fruity Rings"],
    "Garlic": ["Garlic", "garlic"],
    "Garlic B": ["Garlic B"],
    "Garlic China": ["Garlic China"],
    "Garlic Local": ["Garlic Local"],
    "Garlic Small Size": ["Garlic Small Size", "Garlic (የተፈለፈለ)", "Hatched Garlic"],
    "Gebeya Lemne Tikil": ["Gebeya Lemne Tikil", "ገበያ ለምኔ ጥቅል"],
    "Ghion Bleach": ["Ghion Bleach", "Ghion bleach"],
    "Glass Wash": ["Glass Wash", "glass wash"],
    "Ginger": ["Ginger", "Ginger Local", "Ginger Thai"],
    "Girum Cinnamon Tea Bag": ["Girum Cinamon Tea Bag", "Girum Cinnamon Tea Bag ", "Girum Cinnamon Tea Bag"],
    "Glow & Shine Bundle": ["Glow & Shine Bundle"],
    "Gold Water": ["Gold Water"],
    "Green Beans": ["Fossolia", "Green Beans", "Green beans", "Grean Beans", "Green bean", "Fossolia (250g)"],
    "Green Pepper": ["Green Pepper", "green pepper"],
    "Grinded Rosemary": ["Grinded Rosemary"],
    "Groceries": ["Groceries"],
    "Guava": ["Guava"],
    "Gursha Bars": ["Gursha Bars", "Gursha bars"],
    "Habesha Cabbage": ["Habesha Cabbage", "Habesha cabbage", "Gurage cabbage"],
    "Habesha Mitin Shiro": ["Habesha Mitin Shiro", "Habsha Mitin Shiro"],
    "Habsha Pepper": ["Habsha Pepper"],
    "Hamda Powdered Milk": ["Hamda Powdered Milk"],
    "Hand Wash": ["Hand Wash", "hand wash"],
    "Hana Mitmita": ["Hana Mitmita"],
    "Happy Toilet Tissue": ["Happy soft", "happy toilet paper", "Happy Toilet Tissue", "Happy Toilet Paper"],
    "Hassle Free Package": ["Hassle Free Package"],
    "Hilal Tomato Paste": ["Hilal Tomato Paste"],
    "Hilwa Sunflower Oil": ["Hilwa Sunflower Oil"],
    "Holiday Package": ["Holiday Package"],
    "Holy Basil": ["Holy Basil", "Bessobela (holy basil)", "Bessobela", "Holy Basil, በሶ ብላ"],
    "Home 220g Laundry Soap": ["Home 220g Laundry Soap"],
    "Home Ready Package": ["Home Ready Package"],
    "Hoodie": ["Hoodie"],
    "Inci Sunflower Oil": ["Inci Sunflower Oil", "inci oil", "Inci sunflower oil"],
    "Indian Cheers Basmati Rice": ["Indian Cheers Basmati Rice"],
    "Indian Chilly": ["Indian Chilly", "Indian chilly", "Indian chilli", "indian chilly", "Indian chilly "],
    "Indian Golden Basmati Rice": ["Indian Golden Basmati Rice"],
    "Individual Bars": ["Individual Bars", "Individual bars"],
    "Indomie": ["Indomie", "indomie (110g)"],
    "Indomie Noodles (Large)": ["Indomie Noodles (Large)"],
    "Indomie Vegetable Noodles": ["Indomie Vegetable Noodles", "Indomie Vegetable Noodles ", "Indomie Vegie Noodles "],
    "Ipas Anti-Bacterial Soap": ["Ipas Anti-Bacterial Soap", "Ipas antibacterial soap"],
    "Ipas Soap": ["Ipas Soap", "Ipas soap", "ipas soap"],
    "Ipen": ["Ipen"],
    "Jam Comedy Night": ["Jam Comedy Night"],
    "Jimma Coffee": ["Jimma Coffee"],
    "Juliet Biscuit": ["Juliet Biscuit"],
    "Julet Biscut": ["Julet Biscut", "Julet biscut"],
    "Kabana Toiletry Bags": ["Kabana Toiletry Bags"],
    "Kakao Powder": ["Kakao Powder"],
    "Kalos Cookies": ["Kalos Cookies"],
    "Kids Pajama Set": ["Kids Pajama Set"],
    "Kitfo Holiday Package": ["Kitfo Holiday Package"],
    "Kitfo Package": ["Kitfo Package"],
    "Kitfo Spice": ["Kitfo Spice"],
    "Knick Knack Biscut": ["Knick Knack Biscut", "Knick knack biscut", "Knick Knack"],
    "Knitted Blanket": ["Knitted Blanket", "Knitted blanket(Medium)"],
    "Knorr": ["Knorr"],
    "Knorr 5 Piece": ["Knorr 5 Piece", "Knorr /5piece"],
    "Kojj Flour": ["Kojj Flour"],
    "Kojj Pastina": ["Kojj Pastina"],
    "Kono Beauty Soap": ["Kono Beauty Soap", "Kono beauty soap"],
    "Kono Soap": ["Kono Soap", "Kono soap", "kono soap"],
    "Kukulu Package": ["Kukulu Package", " Kukulu Package", "Kukulu package", "Special Kululu Package"],
    "Kuri Lactation Tea": ["Kuri Lactation Tea"],
    "Kushnaye Tikil": ["Kushnaye Tikil", "ኩሽናዬ ጥቅል"],
    "Largo Liquid Detergent": ["Largo Liquid Detergent"],
    "Leather Hand Bags": ["Leather Hand Bags"],
    "Leggings": ["Leggings"],
    "Lemon": ["Lemon", "lemon", "Lomen", "Lemon 250g", "Lime"],
    "Difen Misir": ["Difen Misir", "difen misir", "difen miser", "Difen miser (Imported)", "Sambusa Miser"],
    "Lentils (imported)": ["lentils", "whole lentils", "Lentils (imported)", "Lentils"],
    "Lettuce": ["Lettuce", "Chinese lettuce", "Iceberg Salad", "iceberg salad", "Iceberg", "Lolo rosso"],
    "Lifebuoy Soap": ["Lifebuoy Soap", "Lifebuoy Red (70g)", "life bouy 70g", "Life Buoy (Big)", "Life Buoy ( Big)", "Lifebuoy 70g", "Lifebuoy antibacterial bar soap", "lifebuoy(70g)"],
    "Liquid Cloth Soap": ["Liquid Cloth Soap", "Liquid soap 5l"],
    "Football Club Kit": ["Football Club Kit", "Liverpool 2024/2025 Kit", "Manchester City 2024/2025 Kit", "Manchester United 2024/2025 Kit", "Arsenal 2024/2025 Kit"],
    "Loli Chips": ["Loli chips", "loli chips", "Loli Chips", "Loli Chips ", "Loli Chips Ketchup Flavor", "Loli chips (Paprika)"],
    "Loose Granola (Cereal)": ["Loose Granola (Cereal)", "Loose granola (Cereal)"],
    "Lux Soap": ["Lux Soap", "Lux 70g", "lux 70g"],
    "Lux Soft Touch Soap Bar": ["Lux Soft Touch Soap Bar", "Lux soft touch soap bar"],
    "Ma'ed Bundle": ["Ma'ed Bundle"],
    "Macaroni": ["Macaroni", "macaroni", "Booez Macaroni", "Ok Macaroni"],
    "Maleda Flour": ["Maleda Flour"],
    "Mama's Choice": ["Mama's Choice", "Mama's choice", "Mama's choice with fruits"],
    "Mango": ["Mango", "Ye Habeshaa Mango", "Habesha Mango", "Ye Habesha Mango"],
    "Mango Apple": ["Mango Apple", "Apple Mango"],
    "Mawel Flour": ["Mawel Flour"],
    "Mawi Coffee": ["Mawi Coffee ", "Mawi coffee ", "Mawi Coffee", "Mawi coffee"],
    "Mayra Sunflower Oil": ["Mayra Sunflower Oil", "Mayra sunflower oil"],
    "Mekelesha": ["Mekelesha", "Mekelsha", "My kishin mekelsha 2.5g", "My kishin mekelsha 2g", "My Kishen Mekelesha (2g)", "My Kishin / 3 Pieces", "My kishin / 5 Pieces"],
    "MIA Pasta": ["MIA Pasta", "Mia Pasta", "mia pasta", "Mia pasta "],
    "Minced Meat": ["Minced Meat"],
    "Mint": ["Mint", "mint"],
    "Mix Package": ["Mix Package"],
    "Mix Package Bundle": ["Mix Package Bundle", "Mix package bundle"],
    "Mixed Spice": ["Mixed Spice"],
    "Momin Sunflower Oil": ["Momin Sunflower Oil"],
    "Moon Cinnamon Biscuit": ["Moon Cinnamon Biscuit"],
    "Moon Coffee Biscuit": ["Moon Coffee Biscuit"],
    "Moon Cookies": ["Moon cookies", "moon cookies", "Moon Cookies", "Moon cookies all flavors", "Moon Cookies All Flavors", "Moon cookies cinnamon", "moon cookies cinnamon", "Moon cookies coffee", "moon cookies coffee", "Moon cookies vanilla", "moon cookies vanilla", "Moon Cookies Vanilla"],
    "Moon Strawberry Biscuit": ["Moon Strawberry Biscuit"],
    "Moon Vanilla Biscuit": ["Moon Vanilla Biscuit ", "Moon Vanilla Biscuit", "Moon vanilla biscut ", "Moon vanilla biscut"],
    "Moya Biscut": ["Moya Biscut", "Moya Biscuit"],
    "Moya Biscuit The Saint": ["Moya Biscuit The Saint"],
    "Moya Coco Loops Biscuit": ["Moya Coco Loops Biscuit", "Moya coco loops Biscuit"],
    "Multi Purpose": ["Multi Purpose", "Multi purpose 1L", "Multi purpose 2L"],
    "Nib Chocolate": ["Nib Chocolate", "Nib Bar chocolate", "Nib Chocolate 350 gram", "NIB Chocolate Bar", "NIB Chocolate Spread", "Nib Chocolate Spread", "NIB Dark Mini Chocolate (10 pieces)", "NIB Mini Chocolate", "Nub Mini Chocolate (5pcs)", "Nib chocolate mini", "Nib Mini Chocolate (5pcs)"],
    "Nido 400gram Milk Powder": ["Nido 400gram Milk Powder"],
    "Nigist Sanitary Pad": ["Nigist sanitary pad", "nigist sanitary pad", "Nigist Sanitary Pad"],
    "Nunu Vaseline": ["Nunu Vaseline"],
    "Nura Sunflower Oil": ["Nura Sunflower Oil"],
    "Oche Fasting Butter": ["Oche Fasting Butter"],
    "Ok Macaroni 500g": ["Ok Macaroni (500g)", "Ok Macaroni 500g"],
    "Ok Noodles": ["Ok Noodles", "Ok Vegetable Noodle"],
    "Ok Pasta": ["Ok Pasta"],
    "Ok Vegetable Noodle": ["Ok Vegetable Noodle", "Indomie Vegetable Noodles ", "Indomie Vegetable Noodles", "Indomie Vegie Noodles ", "Indomie Vegie Noodles"],
    "Ok wafers": ["Ok wafers", "Ok Cream Wafer"],
    "Okapi Sunflower Oil": ["Okapi Sunflower Oil"],
    "Omar Sunflower Oil": ["Omaar sunflower oil", "Omar Sunflower Oil"],
    "Omaar Light Meat Tuna (Large)": ["Omaar Light Meat Tuna (Large)", "Omaar light meat tuna(large)"],
    "Omaar Pure Vegetable Ghee": ["omaar ghee(500g)", "Omaar Pure Vegetable Ghee"],
    "Orange Pineapple": ["Orange Pineapple", "Orange Pineapple "],
    "Orange Valencia": ["Orange", "Valencia Orange", "orange Valencia", "Orange Valencia", "Valencia Orange"],
    "Orange Yerer": ["Orange Yerer", "Yerer Orange", "Orange Yarer ", "Orange Yarer"],
    "Organza Shampoo": ["Organza Shampoo"],
    "Orkide Sunflower Oil": ["Orkide Sunflower Oil", "Orkide sunflower oil"],
    "Ox Kircha For 10": ["Ox Kircha For 10", "Ox Kircha - for 10"],
    "Ox Kircha For 6": ["Ox Kircha For 6", "Ox Kircha - for 6"],
    "Papaya": ["Papaya"],
    "Papaya B": ["Papaya B"],
    "Papaya Oversize": ["Papaya Oversize"],
    "Paraffin Hair Oil": ["Paraffin Hair Oil", "Paraffin hair oil", " Paraffin hair oil ", " Paraffin hair oil"],
    "Pasta": ["Pasta", "pasta"],
    "Pea": ["Pea"],
    "Pencil": ["Pencil"],
    "Pepper": ["Pepper", "Red Pepper", "Yellow Pepper"],
    "Pineapple": ["Pineapple", "pineapple"],
    "Pineapple B": ["Pineapple B"],
    "Pop Corn": ["Pop Corn", "Pop corn"],
    "Potato": ["Potato", "Potatoes", "Potatoes Restaurant quality", "Potatoes Restaurant Quality", "Potato B"],
    "Potato Chips": ["Potato for Chips", "Potato Chips", "Potatoes Chips"],
    "Potato Grade C": ["Potato Grade C", "Potato C"],
    "Power Tissue": ["Power Tissue"],
    "Prima Macaroni": ["Prima Macaroni"],
    "Prima Pasta": ["Prima Pasta"],
    "Printed T-Shirt": ["Printed T-Shirt", "Printed T-shirt", "printing T-shirt"],
    "Pumpkin": ["Pumpkin"],
    "Pure Iodized Salt": ["Pure Iodized Salt"],
    "Pure Salt": ["Pure salt", "Pure Salt ", "Pure Salt"],
    "Quaker White Oats": ["Quaker White Oats"],
    "Quanta": ["Quanta", "ET Quanta"],
    "Qulet Package": ["Qulet Package", "Qulet package ", "Qulet package", "ቁሌት ጥቅል ", "ቁሌት ጥቅል", "Special qulet package"],
    "Radical Exercise Book": ["Radical Exercise Book", "Radical Exercise Book/ Pack", "radical exercise book", "Radical Exercise Book/50p"],
    "Rahmet Flour": ["Rahmet Flour"],
    "Raw Pop Corn": ["Raw Pop Corn", "Raw Popcorn 500g"],
    "Red Onion (ሃበሻ)": ["Red Onion (ሃበሻ)", "Red onion ( ሃበሻ )", "Red onion ( ሃበሻ ) ", "Red Onion habesha"],
    "Red Onion A": ["Red Onion A", "Redonion A ", "Redonion A", "Red Onion Grade A", "Red Onion Grade A Restaurant q", "Red Onion Grade A Restaurant quality", "Red Onion", "Red Onion Qelafo"],
    "Red Onion B": ["Red Onion B", "Red Onion Grade B", "Redonion B"],
    "Red Onion C": ["Red Onion C", "Red Onion Grade C", "Redonion C" ,"Pilled Red onion"],
    "Red Onion D": ["Red Onion D"],
    "Red Onion Elfora": ["Red Onion Elfora", "Redonion Elfora", "Red onion elfora", "Red onion elfora "],
    "Red Rose Basmati Rice": ["Red Rose Basmati Rice"],
    "Regular Goat Package": ["Goat Meat", "የፍየል ስጋ", "Special Goat Package", "Regular Goat Package", "ሙክት የፍየል ስጋ"],
    "Rib Stylish T-Shirt": ["Rib Stylish T-Shirt", "Rib Stylish T-shirt"],
    "Rice": ["Rice", "rice"],
    "Roll Detergent powder": ["Rol Bio Laundry powder ", "Rol Bio Laundry powder", "Roll Detergent powder"],
    "Rosa Pasta": ["Rosa Pasta", "Booez Pasta", "Mawel pasta", "Alvima Pasta"],
    "Rose Laundry Soap": ["Rose Laundry Soap", "Rose laundry soap"],
    "Rose Laundry Soap (250g)": ["Rose Laundry Soap (250g)"],
    "Rosemary": ["Rosemary"],
    "Rosmery": ["Rosmery"],
    "Safa Ketchup": ["Safa Ketchup"],
    "Safa tomato paste": ["Safa tomato paste", "safa tomato paste", "Safa Tomato Paste", "Safa Tomato Paste 400g"],
    "Safe Soft": ["Safe Soft"],
    "Salsa Tomato Paste": ["Salsa Tomato Paste", "Salsa Tomato Paste (400g)", "salasa tomato paste ", "salasa tomato paste"],
    "Salad": ["Salad"],
    "Semira Basmti Rice": ["Semira Basmti Rice"],
    "Sheep package": ["Lamb", "Lamp", "የበግ ስጋ", "ጠቦት የበግ ስጋ", "Regular Mutton Package", "Sheep package", " Mutton", "Mutton"],
    "Shega Red Teff": ["Shega Red Teff"],
    "Shega White Teff": ["Shega White Teff", "Shega Teff", "Shega White Teff\\t"],
    "Sheno Lega Ghee": ["Butter", "Table Butter", "Shola Table butter", "Tycoon Table Butter", "sheno lega ghee", "Sheno Lega Ghee"],
    "Signal Toothpaste (Medium)": ["Signal Toothpaste (Medium)"],
    "Simba Tea Bag (Thyme)": ["Simba Tea Bag (Thyme)", "Simaba tea bag tyhme", "Simaba tea bag tyhme ", "simba tea bag(thyme)"],
    "Simba Tea bag": ["Simba Tea bag", "Simba tea bag", "Simba Tea Bag", "Simba tea bag(thyme)"],
    "Simba Tomato Paste": ["Simba Tomato Paste", "Simba tomato paste "],
    "Small Bites Pack": ["Small Bites Pack", "Small bites pack"],
    "Small Red Onion": ["Small Red Onion"],
    "Small Size Beetroot": ["Small Size Beetroot", "Small & Big Size Beetroot"],
    "Small Size Carrot": ["Small Size Carrot", "Small Carrot", "Small size Carrot"],
    "Small Size Potato": ["Small size Potato", "Small Size Potato ", "Small Size Potato"],
    "Snail Hair Oil": ["Snail Hair Oil", "Snail hair oil"],
    "Solar Laundry Soap": ["Solar Laundry Soap", "Solar Laundry soap"],
    "Sosi": ["Sosi"],
    "Sosi Soya": ["Sosi Soya"],
    "Special Mix Package": ["Special Mix Package", "Special Mix package"],
    "Special Mutton Package": ["Special Mutton Package"],
    "Spinach": ["spinach", "Spinach", "Swiss chard", "Swiss Chard", "Kale"],
    "Squash": ["squash", "Squash"],
    "Strawberry": ["Strawberry"],
    "Sugar": ["Sugar", "Sugar (1kg)"],
    "Sun Chips": ["Sun Chips", "Sun chips", "sun chips", "Sun Chips (30g)"],
    "Sunflower Seeds": ["Sunflower Seeds", "Sunflower seeds", "Sunflower Seeds(Suf)", "sunflower seeds"],
    "Sunny Bleach": ["Sunny Bleach", "Sunny Bleach / 5L", "Sunny Bleach 5 Liter", "Sunny Bleach / 800ML"],
    "Sunny Body Jel": ["Sunny Body Jel"],
    "Sunny Detergent": ["Sunny Detergent", "Sunny Detergent / 5L", "Sunny Detergent / 800ML"],
    "Sunny Dish Wash": ["Sunny Dish Wash"],
    "Sunny Hand Wash": ["Sunny Hand Wash"],
    "Sunny Multi Purpose": ["Sunny Multi Purpose"],
    "Sunny Oxidizer": ["Sunny Oxidizer", "Sunny Oxidizer"],
    "Sunny Shampoo": ["Sunny Shampoo"],
    "Sunny Softener": ["Sunny Softener", "Sunny softener"],
    "Sunny Window Cleaner": ["Sunny Window Cleaner", "Sunny Window Cleaner"],
    "Sunsilk Coconut Shampoo": ["Sunsilk Coconut Shampoo", "Sunsilk coconut Shampoo"],
    "Sunsilk Conditioner": ["Sunsilk Conditioner", "Sunsilk conditioner", "Sunsilk conditioner Big"],
    "Sunsilk Shampoo": ["Sunsilk shampoo 350ml", "sunsilk shampoo 350ml", "Sunsilk Shampoo 350ml", "Sunsilk shampoo Big", "Sunsilk Shampoo"],
    "Sunvito Sunflower Oil": ["Sunvito Sunflower Oil"],
    "Sweet Potato": ["Sweet Potatoes", "sweet potato", "Sweet potato", "Sweet Potato"],
    "T-500 Smartwatch": ["T-500 Smartwatch"],
    "T-Shirt": ["T-Shirt", "T-shirt"],
    "Taflen Paraffin Hair Oil": ["Taflen Paraffin Hair Oil"],
    "Take Away Box": ["Take Away Box", "Take away box"],
    "Tasty Soya": ["Tasty Soya"],
    "Taza Granola": ["Taza Granola"],
    "Taza Oats": ["Taza Oats", "Taza oats"],
    "Taza Quick Oats": ["Taza Quick Oats", "Taza Quick oats"],
    "Tea Spice": ["Tea Spice"],
    "Tea Time Bundle": ["Tea Time Bundle"],
    "Teff Flakes": ["Teff Flakes"],
    "Tena Cooking Oil": ["Tena Cooking Oil", "Tena Cooking oil", "Tena cooking oil 1L", "Tena cooking oil 5L"],
    "Tesfaye Peanut Butter": ["Tesfaye Peanut Butter", "Tesfaye Peanut"],
    "Theday Strawberry Jam": ["Theday strawberry jam", "theday strawberry jam", "Theday Strawberry Jam"],
    "Toiletry Hand Bags": ["Toiletry Hand Bags"],
    "Tomato A": ["Tomato A", "Tomatoes Grade A", "Tomato", "Tomato Restaurant Quality ", "Tomato Restaurant Quality", "Tomatoes A", "Tomato Grade A"],
    "Tomato B": ["Tomato B", "Tomatoes B", "Tomatoes Grade B"],
    "Tomato Ripe": ["Tomato/ Ripe/ Small size /", "Tomato Ripe"],
    "Tomato Roma": ["Tomato Roma", "Tomato Beef"],
    "Tooth Brush": ["Tooth brush", "Tooth Brush ", "Tooth Brush"],
    "Top Bottled Water": ["Top Bottled Water"],
    "Torti Chips": ["Torti Chips", "Torti Chips (30g)"],
    "Tote Bag": ["Tote Bag", "Tote bag"],
    "Tulip Toilet Paper": ["Tulip Toilet Paper", "Tulip soft ", "Tulip soft"],
    "Tumha Dish Wash": ["Tumha Dish Wash", "Tumha Dish Wash"],
    "Tumha Hand Wash": ["Tumha Hand Wash"],
    "Tumha Laundry Detergent": ["Tumha Laundry Detergent"],
    "Ture Red Teff": ["Ture Red Teff"],
    "Ture White Teff": ["Ture White Teff"],
    "Turmeric": ["Turmeric"],
    "Turtle Neck": ["Turtle Neck"],
    "Twins Facial Tissue": ["Twins Facial Tissue"],
    "Twins Paper Towel": ["Twins Paper Towel"],
    "Twins Table Napkin": ["Twins Table Napkin"],
    "Twins Toilet Tissue": ["Twins Toilet Tissue"],
    "Two Piece Kids Pajama Set": ["Two Piece Kids Pajama Set"],
    "Vanilla Flavoring Essence": ["Vanilla Flavoring Essence", "Vanilla flavoring essence"],
    "Varika Shampoo": ["Varika Shampoo", "Varika shampoo"],
    "Vaseline": ["Vaseline", "Vasline"],
    "Vaseline Coco Radiant": ["Vaseline Coco Radiant", "Original Vaseline Coco Radiant 200ml"],
    "Vaseline Coconut Lotion 200ml": ["Vaseline Coconut Lotion 200ml", "Vasline cocnut lotion 200ml"],
    "Vaseline Dry Skin 200ml": ["Vaseline Dry Skin 200ml", "Vasline dry skin 200ml"],
    "Vaseline Dry Skin Repair": ["Vaseline Dry Skin Repair", "Original Vaseline Dry Skin Repair 200ml"],
    "Vaseline PJ Original": ["Vaseline PJ Original", "Vaseline Pj Original 45ml"],
    "Vatika Almond Hair Oil": ["Vatika Almond Hair Oil"],
    "Vatika Black Seed Hair Oil": ["Vatika Black Seed Hair Oil", "Vatika Hail Oil (Black Seed)"],
    "Vatika Coconut Hair Oil": ["Vatika Coconut Hair Oil"],
    "Vatika Garlic Hair Oil": ["Vatika Garlic Hair Oil", "Vatika Hairl Oil (Garlic)"],
    "Vatika Hair Oil": ["Vatika Hair Oil", "Vatika hair oil"],
    "Vatika Henna Shampoo": ["Vatika Henna Shampoo"],
    "Vatika Lemon Shampoo": ["Vatika Lemon Shampoo"],
    "Vatika Olive Hair Oil": ["Vatika Olive Hair Oil"],
    "Vatika Shampoo": ["Vatika Shampoo"],
    "Vegetable Package Bundle": ["Vegetable Package Bundle", "Vegetable package  bundle"],
    "Victory Water": ["Victory water 1L", "Victory water 2L", "Victory Natural Water/Pack", "Victory Water ", "Victory Water", "victory purified natural water"],
    "Viva Toilet Paper Tissue": ["Viva Toilet Paper Tissue"],
    "Wakene 1st Level Flour": ["wakene flour", "Wakene flour", "Wakene 1st Level Flour"],
    "Wallet": ["Wallet"],
    "Watermelon": ["Watermelon"],
    "White Cabbage": ["White Cabbage", "White Cabbage (Large)", "White Cabbage (large)", "Habesha cabbage", "White Cabbage (Small)", "White Cabbage (small)", "White Cabbage (medium)", "whitecabbage", "White cabbage", "Cabbage"],
    "White Onion": ["White Onion A", "White Onion B", "White Onion C", "White Onion"],
   
    "White Cabbage B": ["White Cabbage B"],
    "Window Cleaner": ["Window Cleaner", "window cleaner"],
    "Wild Coffee": ["Wild Coffee"],
    "Wush Wush Tea": ["wush wush tea", "Wush Wush Tea"],
    "YALI Bleach": ["YALI Bleach", "YALI Bleach 5lit ", "YALI Bleach 5lit", "Yali Bleach 5L", "YALI Bleach 1 Lit", "YALI Bleach 350ml"],
    "YALI Dish Wash": ["YALI Dish Wash", "YALI Dish Wash 5lit", "YALI Dish Wash 750ml"],
    "YALI Hand Wash": ["YALI Hand Wash", "YALI Hand Wash 500ml", "YALI Hand Wash 5lit"],
    "Yali Laundry Detergent": ["Yali Laundry Detergent", "YALI Laundary Detergent 5lit", "Yali Laundry detergent 5lit"],
    "YALI Multi Purpose": ["YALI Multi Purpose", "YALI Multi Purpose 2lit ", "YALI Multi Purpose 2lit", "Yali Multi purpose 2lit", "YALI Multi Purpose 1lit", "YALI Multi Purpose 5lit", "Yali Multi purpose 5lit"],
    "YALI Powder Cleaner": ["YALI Powder Cleaner", "YALI Powder cleaner"],
    "YALI Window Cleaner": ["YALI Window Cleaner", "YALI Window Cleaner 750ml"],
    "Yeah laundry bar soap": ["YEAH Bar Soap", "Yeah Bar Soap", "Yeah laundry bar soap"],
    "Yellow Pea": ["Yellow Pea", "yellow pea", "yellow peas"],
    "Yes Water": ["Yes Water", "Yes water 0.33lt", "Yes Water 1L", "Yes water 2L", "Yes water 500ml", "Yes Water (2L)", "Yes Water (Pack)", "Yes Water 0.33ml (Pack)"],
    "Yon-X Laundry Detergent": ["Yon-X Laundry Detergent"],
    "Yon-X Liquid Dish Wash": ["Yon-X Liquid Dish Wash"],
    "Yummy Crunchy": ["Yummy Crunchy", "Yummy crunchy"],
    "Yummy Crunchy & Rasins 500g": ["Yummy Crunchy & Rasins 500g", "Yummy crunchy & rasins 500g", "Yummy Crunchy & Raisins"],
    "Yummy Package": ["Yummy Package"],
    "Yummy Peanut Butter": ["Yummy Peanut Butter", "Yummy Crunchy Peanut Butter", "Yummy Smooth Peanut Butter"],
    "Zagol Cheese": ["Zagol Cheese"],
    "Zagol Milk": ["Zagol Milk", "Zagol Milk"],
    "Zagol Table Butter": ["Zagol Table Butter"],
    "Zagol Yoghurt": ["Zagol Yoghurt", "Zagol yoghurt"],
    "Zalash Hair Oil": ["Zalash Hair Oil", "Zalash hair oil"],
    "Zehabu Sunflower Oil": ["Zehabu Sunflower Oil"],
    "Zenith Hair Oil": ["Zenith Hair Oil", "Zenith hair oil"],
    "Zion Biscut": ["Zion Biscut", "Zion biscut", "Zion biscuit"],
    "Zucchini": ["Zucchini", "Courgette", "Courgette ","Courgetti"],
}

def _create_child_to_parent_map(mapping):
    child_map = {}
    for parent, children in mapping.items():
        for child in children:
            cleaned_child = re.sub(r'\s+', ' ', child).strip().lower()
            child_map[cleaned_child] = parent
    return child_map

CHILD_TO_PARENT_MAP = _create_child_to_parent_map(PARENT_CHILD_MAPPING)

def _generate_stable_uuid(name):
    return str(uuid.uuid5(NAMESPACE_UUID, name))

def create_parent_child_master_table(all_product_data_df: pd.DataFrame) -> pd.DataFrame:
    print("\n--- Generating hub_standard_products Table (Option 3) ---")

    if all_product_data_df.empty:
        print(" -> Input data is empty. No master table to generate.")
        return pd.DataFrame()

    df = all_product_data_df.copy().dropna(subset=['raw_product_name'])
    
    initial_count = len(df)
    df = df[df['raw_product_name'].astype(str).str.strip() != '0']
    
    garbage_values = ['item name', 'product name', 'test', 'n/a', 'na', 'none', '', 'white onion', 'white onion a', 'white onion b', 'white onion c']
    df = df[~df['raw_product_name'].astype(str).str.strip().str.lower().isin(garbage_values)]
    
    final_count = len(df)
    if initial_count > final_count:
        print(f" -> Filtered out {initial_count - final_count} invalid/garbage records.")

    print("Step 1: Mapping products to parents using PARENT_CHILD_MAPPING...")
    df['cleaned_name'] = df['raw_product_name'].apply(
        lambda x: re.sub(r'\s+', ' ', str(x)).replace(''', "'").replace(''', "'").replace('`', "'").strip().lower()
    )
    df['parent_name'] = df['cleaned_name'].map(CHILD_TO_PARENT_MAP)
    
    df['parent_name'] = df['parent_name'].fillna(df['raw_product_name'])

    print("Step 2: Converting created_at to consistent format...")
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
    
    print("Step 3: Aggregating by parent to get earliest created_at and source...")
    parent_groups = df.groupby('parent_name').agg({
        'created_at': lambda x: x.dropna().min() if not x.dropna().empty else None,
        'source_db': lambda x: x.iloc[0] if len(x) > 0 else None
    }).reset_index()
    
    print("Step 4: Generating parent_product_id...")
    parent_groups['parent_product_id'] = parent_groups['parent_name'].apply(_generate_stable_uuid)
    
    print("Step 5: Finalizing canonical master table...")
    canonical_df = parent_groups[[
        'parent_product_id',
        'parent_name',
        'source_db',
        'created_at'
    ]].copy()
    
    canonical_df = canonical_df.rename(columns={'source_db': 'source'})
    canonical_df = canonical_df.sort_values('parent_name').reset_index(drop=True)
    
    print(f" -> Created hub_standard_products with {len(canonical_df)} parent products.")
    print(f" -> Child products are linked via parent_product_id in source tables.")
    return canonical_df

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def main():
    try:
        supabase_engine = get_db_engine('supabase')
        hub_engine = get_db_engine('hub')
        
        print("\nCONNECTING TO REMOTE SOURCES...")

        print("Fetching from Remote Supabase (products)...")
        with supabase_engine.connect() as conn:
            df_sup_p = pd.read_sql("SELECT id as raw_product_id, name as raw_product_name, created_at FROM products", conn)
        print(f"Supabase: Fetched {len(df_sup_p)} products.")

        from pipeline.data_loader import load_product_data_from_staging
        
        staging_engine = get_db_engine('staging')
        df_staging = load_product_data_from_staging(staging_engine)
        
        all_product_data = pd.concat([df_sup_p, df_staging], ignore_index=True)
        
        master_df = create_parent_child_master_table(all_product_data)

        if master_df.empty:
            print("Standardization failed. No data to save.")
            return

        print("\nCreating mapping lookup...")
        
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
