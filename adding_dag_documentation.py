"""

Some workflows can be quite complex as they grow over time.
Hence, it is useful to add key-points description for every DAG in the Airflow UI,
so the most important information can be found at ease without going through X wiki-pages.

To improve the documentation of our DAGs and improve knowledge sharing, I added the following structure below to our codebase.
The reason for unraveling the basic structure of the "DAG documentation" is to limit "freestyle documentation" by restricting available sections to fill in.
Example texts are given for inspiration on what information might be useful for each section.

summary: Crisp description about what is happening in this DAG (2-3 sentences are sufficient)
on failure actions: What should you do, if this DAG fails? Anybody you need to notify?
internal contact: This is set to e-mail the responsible squad per default but can be changed if applicable

The HTML-converter abstractions are helper-functions for colleagues that are not familiar with HTML and also improves the readability of code.

"""

# Separate function in a utils folder:

from textwrap import dedent

def dag_doc(
        summary: str,
        on_failure_actions: str,
        internal_contact='<a href="mailto: CONTACTEMAILADRESS?subject=Contact request regarding DAG: *insert DAG-name here*" target="_blank">TEAM NAME</a>'
) -> str:

    dag_doc_content = f"""
#### DAG Summary
{dedent(summary)}

#### On Failure Actions
{dedent(on_failure_actions)}

#### Internal Contact
{dedent(internal_contact)}
    """

    return dag_doc_content


def create_html_link(link_text: str, url: str) -> str:
    return f'<a href = "{url}" target = "_blank" >{link_text}</a>'


def create_html_email_link(link_text: str, url: str) -> str:
    return f'<a href = "mailto: {url}" target = "_blank" >{link_text}</a>'
    
    
# In the actual workflow/dag file with example text:
from airflow import DAG
from utils.xxxxx import dag_doc, create_html_link, create_html_email_link

dag.doc_md = dag_doc(
        summary=f"""
            This DAG transforms raw data from {create_html_link(link_text='DATA SOURCE', url='DATA SOURCE URL')}
            by extracting and harmonising XYZ and ZYX data. <br> The data is fetched through a scheduled
            {create_html_link(link_text='DATA SOURCE FETCH', url='FETCH SCHEDULER LINK')} job and
            automatically streamed to {create_html_link(link_text='DATA WAREHOUSE', url='DW-LINK')} via a S3 ingest.

            *Datazone of datasets*: source: XX -> destination: YY
        """,
        on_failure_actions=f"""
            Clear all tasks and rerun DAG.
            If it continues to fail, notify XYZ Squad.

            If the issue is related to {create_html_link(link_text='DATA SOURCE', url='DATA SOURCE URL')},
            please notify {create_html_email_link(link_text='CONTACT PERSON', url='CONTACT PERSON EMAIL')}.
        """
    )

