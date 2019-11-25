def tabulate(l, headers=list()):
    """
        List -> String
        Consumes a list and returns a tabulated string
        or, Consumes a list of tuples and returns a tabulated string
    """
    headerString = ""
    for h in headers:
        headerString += "{:<60}".format(h)
        
    bodyString = ""
    for i in l:
        bodyString += "\n"
        if(type(i) is tuple):
            for c in i: 
                bodyString += "{:<60}".format(c)
        else:
            bodyString += "{:<60}".format(i)

    return headerString + bodyString

def htmlify(l, headers=list(), size='s'):
    """
        List -> String
        Consumes a list and returns an HTML string
        or, Consumes a list of tuples and returns an HTML string
    """
    
    width=400
    if(size == 'l'):
        width = 800

    html = f"<table style='width:{width};border:1px solid silver'>"

    headerString = "<tr>"
    for h in headers:
        headerString += f"<th>{h}</th>"
    headerString += "</tr>"
        
    bodyString = ""
    align = 'right'
    for i in l:
        bodyString += "<tr>"
        if(type(i) is tuple):
            for c in i: 
                try:
                    float(c)
                    align = 'right'
                except:
                    align = 'left'

                bodyString += f"<td  align='{align}'>{c}</td>"
        else:
            try:
                float(i)
                align = 'right'
            except:
                align = 'left'
            bodyString += f"<td align='{align}'>{i}</td>"
        bodyString += "</tr>"

    html += headerString + bodyString + "</table>"

    return html


    
