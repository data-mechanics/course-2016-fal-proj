propval = gross_tax > 0
foodpan = all food pantries

pick some threshold and select all those prop vals with gross_tax < threshold
threshhold = avg gross_tax

low_tax = gross_tax < threshold

combine low_tax with food pan if same zipcode

same_zip = low_tax ~union~ foodpan if zipcode == same