import { DashboardComponent } from "@/components/dashboard";
import Image from "next/image";
import 'dotenv/config'
import { config } from "dotenv";
import { Metadata } from 'next';

const API_ENDPOINT = process.env.API_ENDPOINT

export const metadata: Metadata = {
  title: 'All Databases',
};

export default async function Home() {
    config()
    console.log(API_ENDPOINT)
  
    let response = await fetch(`${API_ENDPOINT}/get_database_list`)
    let data = await response.json()

    return (
    <main>
      <DashboardComponent data={data}/>
    </main> 
  );
}
