'use client'

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { DatabaseIcon, SearchIcon } from 'lucide-react'
import Link from 'next/link'

export async function DashboardComponent({data}) {
  const [searchTerm, setSearchTerm] = useState('')
  
  const filteredDatabases = data.filter(db =>
    db.name.toLowerCase().includes(searchTerm.toLowerCase())
  )

  return (
    <div className="container mx-auto p-4">
      <h1 className="text-2xl font-bold mb-6">Database Insights Dashboard</h1>
      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Database Overview</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex justify-between items-center mb-4">
            <div className="flex items-center space-x-2">
              <Input
                placeholder="Search databases..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-64"
              />
              <Button size="icon">
                <SearchIcon className="h-4 w-4" />
              </Button>
            </div>
            <Button>
              <DatabaseIcon className="mr-2 h-4 w-4" /> Add New Database
            </Button>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Database Name</TableHead>
                <TableHead>Total Measurements</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredDatabases.map((db) => (
                <TableRow key={db.id}>
                  <TableCell>{db.name}</TableCell>
                  <TableCell>{db.measurement_count}</TableCell>
                  <TableCell>
                    <Link href={`/insights/${db.id}`} passHref>
                      <Button variant="outline" size="sm">View Insights</Button>
                    </Link>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}