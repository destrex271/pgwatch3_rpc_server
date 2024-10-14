"use client"

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { ArrowLeft, Search, Filter } from 'lucide-react'
import Link from 'next/link'

interface Insight {
  id: string
  data: string
  created_time: string
  db_id: string
}


export function InsightsComponent({data, db_id}) {
  const [searchTerm, setSearchTerm] = useState('')
  console.log(data)

  return (
    <div className="container mx-auto p-4">
      <div className="flex items-center mb-6">
        <Link href="/" passHref>
          <Button variant="outline" size="sm" className="mr-4">
            <ArrowLeft className="mr-2 h-4 w-4" /> Back to Dashboard
          </Button>
        </Link>
        <h1 className="text-2xl font-bold">Insights for Database {db_id}</h1>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Generated Insights</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex justify-between items-center mb-4">
            <div className="flex items-center space-x-2">
              <Input
                placeholder="Search insights..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-64"
              />
              <Button size="icon" aria-label="Search">
                <Search className="h-4 w-4" />
              </Button>
            </div>
            <div className="text-sm text-muted-foreground">
              {data != null ? data.length: 0} insights found
            </div>
          </div>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Id</TableHead>
                  <TableHead>Description</TableHead>
                  <TableHead className="text-right">Created At</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data != null && data.map((insight) => (
                  <TableRow key={insight["id"]} className="py-2">
                    <TableCell className="text-left p-5" style={{borderBottom:"1px solid #00000022"}} >{insight["id"]}</TableCell>
                    <TableCell style={{border:"1px solid #00000022"}}>
                      <div style={{ overflowX: "auto" }}>
                        <pre style={{ whiteSpace: "pre-wrap", wordWrap: "break-word" }}>
                          {insight["data"]}
                        </pre>
                      </div>
                    </TableCell>
                    <TableCell className="text-right" style={{borderBottom:"1px solid #00000022"}} >{new Date(insight["created_time"]).toLocaleString()}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          {data == null || data.length === 0 && (
            <div className="text-center py-4 text-muted-foreground">
              No insights found. Try adjusting your search or filter.
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}