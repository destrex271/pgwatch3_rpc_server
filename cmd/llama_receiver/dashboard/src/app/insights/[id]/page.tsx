import { InsightsComponent } from "@/components/insights";
import { Metadata } from "next";

export const metadata: Metadata = {
  title: 'Insgiht for DB',
};

export default async function Page({ params }) {
    let id = params.id
    
    const API_ENDPOINT = process.env.API_ENDPOINT
    let response = await fetch(`${API_ENDPOINT}/get_database_insight?db_id=${id}`)
    let jsonData = await response.json()
    
    if(response.status == 500){
      return(
        <main>
          {jsonData}
        </main>
      )
    }
    
    if(response.status == 404){
      return(
        <main>
          <InsightsComponent data={null} db_id={id} />
        </main>
      )
    }

    return (
    <main>
      <InsightsComponent data={jsonData} db_id={id}/>
    </main> 
  );
}