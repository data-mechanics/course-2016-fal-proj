Cody Karjadi and John Gonsalves Project 1 README

Narrative:

Why do people live in a given area? This is a complex question that can be approached from the bottom-up; considering factors like schools, proximity to public transportation, and cost of living. This question can also be considered from the top-down; meaning we consider what effects come from certain people grouping in certain areas. For example, perhaps in an area with a high property value there will be a lot of fancy restaurants. And perhaps an area with low property value will have a lot of hospitals, fast food, and food pantries. Beginning to characterize areas by following a top-down approach is the start to understanding why people live in a given area.

Project 1:

Run retrieve.py after running mongo with authentication enabled
Run same_street, same_zip, or same_city in any order; these will input the .same_street, Food_Prop_Hosp_ZipCodes, and .commGarden_foodEstabl collections, respectively.

Limitations: same_zip currently only collects data for zip_codes that have at least 1 hospital, food pantry, and properties. same_street
currently only collects data for streets that have had both a waze jam and active work zone, or streets that have had at least a waze jam.
same_city collects data for cities that have community gardens, and cities that have community gardens and fast food restaurants.

In future iterations of the project we will collect data for all zip_codes that have at least 1 of any of the 3 criteria, not all 3. And for
same_street we will collect data for streets have had at least 1 of either a jam or an active work zone. For same_city we will consider cities with
no community gardens.

The actual relations or correlations from project 1 aren't really clear, it will take more refinement from different iterations and more definite algorithms to create any real correlations, but these functions show that we can manipulate given data in different ways.