'use client'

import { useState } from 'react'
import { useRouter } from 'next/router'
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { ArrowLeftIcon, SearchIcon } from 'lucide-react'
import Link from 'next/link'

interface Insight {
  id: string
  title: string
  description: string
  createdAt: string
  category: string
}

const mockInsights: Insight[] = [
  { id: '1', title: 'High Customer Churn Rate', description: 'Customer churn rate has increased by 15% in the last quarter.', createdAt: '2023-09-15T14:30:00Z', category: 'Customer Retention' },
  { id: '2', title: 'Product A Sales Spike', description: 'Product A sales have increased by 30% in the past month.', createdAt: '2023-09-14T09:45:00Z', category: 'Sales Performance' },
  { id: '3', title: 'New User Signup Trend', description: 'New user signups have doubled since the launch of the referral program.', createdAt: '2023-09-13T11:10:00Z', category: 'User Acquisition' },
  { id: '4', title: 'Customer Satisfaction Improvement', description: 'Overall customer satisfaction score has improved by 10 points.', createdAt: '2023-09-12T16:20:00Z', category: 'Customer Satisfaction' },
]

export function InsightsComponent() {
  const [searchTerm, setSearchTerm] = useState('')
  const router = useRouter()
  const { id } = router.query

  const filteredInsights = mockInsights.filter(insight =>
    insight.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
    insight.description.toLowerCase().includes(searchTerm.toLowerCase())
  )

  return (
    <div className="container mx-auto p-4">
      <div className="flex items-center mb-6">
        <Link href="/" passHref>
          <Button variant="outline" size="sm" className="mr-4">
            <ArrowLeftIcon className="mr-2 h-4 w-4" /> Back to Dashboard
          </Button>
        </Link>
        <h1 className="text-2xl font-bold">Insights for Database {id}</h1>
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
              <Button size="icon">
                <SearchIcon className="h-4 w-4" />
              </Button>
            </div>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Title</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Category</TableHead>
                <TableHead>Created At</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredInsights.map((insight) => (
                <TableRow key={insight.id}>
                  <TableCell className="font-medium">{insight.title}</TableCell>
                  <TableCell>{insight.description}</TableCell>
                  <TableCell>{insight.category}</TableCell>
                  <TableCell>{new Date(insight.createdAt).toLocaleString()}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}