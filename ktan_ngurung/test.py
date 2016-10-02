import pdfquery 
from xml.etree.ElementTree import ElementTree 

pdf = pdfquery.PDFQuery("2014-bluebook-ridership.pdf")
pdf.load()
pdf.tree.write("test.xml", pretty_print=True, encoding="utf-8")
tree = ElementTree()
tree.parse("test.xml")
print(tree)